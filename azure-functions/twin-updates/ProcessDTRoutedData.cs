// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}

using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Azure;
using Azure.Core.Pipeline;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Net.Http;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Threading.Tasks;
using System.Collections.Generic;
using TwinUpdatesSample.Dto;
using System.Threading;

namespace TwinUpdatesSample
{
    public class ProcessDTRoutedData
    {
        private static HttpClient _httpClient = new HttpClient();
        private static string _adtServiceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");

        /// <summary>
        /// The outcome of this function is to get the average floor temperature and humidity values based on the rooms on that floor. 
        /// 
        /// 1) Get the incoming relationship of the room. This will get the floor twin ID
        /// 2) Get a list of all the rooms on the floor and get the humidity and temperature properties for each
        /// 3) Calculate the average temperature and humidity across all the rooms
        /// 4) Update the temperature and humidity properties on the floor
        /// </summary>
        /// <param name="eventGridEvent"></param>
        /// <param name="log"></param>
        /// <returns></returns>

        [FunctionName("ProcessDTRoutedData")]
        public async Task Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            log.LogInformation("ProcessDTRoutedData (Start)...");
            
            DigitalTwinsClient client;
            DefaultAzureCredential credentials;

            // if no Azure Digital Twins service URL, log error and exit method 
            if (_adtServiceUrl == null)
            {
                log.LogError("Application setting \"ADT_SERVICE_URL\" not set");
                return;
            }

            try
            {
                //Authenticate with Azure Digital Twins
                credentials = new DefaultAzureCredential();
                client = new DigitalTwinsClient(new Uri(_adtServiceUrl), credentials, new DigitalTwinsClientOptions { Transport = new HttpClientTransport(_httpClient) });
            }
            catch (Exception ex)
            {
                log.LogError($"Exception: {ex.Message}");

                client = null;
                credentials = null;
                return;
            }

            if (client != null)
            {
                if (eventGridEvent != null && eventGridEvent.Data != null)
                {
                    JObject message = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());

                    log.LogInformation($"Event Grid Event received: {eventGridEvent.EventType}");
                    log.LogInformation($"Event Grid Event received: {eventGridEvent.Data}");

                    string twinId = eventGridEvent.Subject.ToString();
                    log.LogInformation($"TwinId: {twinId}");

                    string modelId = message["data"]["modelId"].ToString();
                    log.LogInformation($"ModelId: {modelId}");

                    // if the twin is a Sensor
                    // ignore all others
                    if (modelId.StartsWith("dtmi:ttnlwstack")) // TODO: this is a dirty hack and may not retrieve all sensors :)
                    {
                        if (eventGridEvent.Data.ToString().Contains("environmental_info"))
                        {
                            log.LogInformation($"Ignore environmental_info update");
                            return;
                        }

                        log.LogInformation($"Consolidating EnvironmentalInfo for the sensor");

                        EnvironmentalInfo environmentalInfo = new EnvironmentalInfo();

                        // small delay to make sure twin is up-to-date when we will query it
                        Thread.Sleep(3000);

                        // get the Sensor twin
                        AsyncPageable<BasicDigitalTwin> queryResponse = client.QueryAsync<BasicDigitalTwin>($"SELECT * FROM digitaltwins WHERE $dtId = '{twinId}'");

                        await foreach (BasicDigitalTwin sensorTwin in queryResponse)
                        {
                            JObject decodedPayload = (JObject)JsonConvert.DeserializeObject(sensorTwin.Contents["decodedPayload"].ToString());

                            environmentalInfo.temperature = Convert.ToDouble(decodedPayload["temperature"] ?? decodedPayload["temp"] ?? decodedPayload["TEMP"] ?? decodedPayload["TempC_SHT"] ?? null);
                            environmentalInfo.humidity = Convert.ToDouble(decodedPayload["humidity"] ?? decodedPayload["Hum_SHT"] ?? decodedPayload["relativeHumidity"] ?? decodedPayload["RHUM"] ?? null);
                            environmentalInfo.co2 = Convert.ToDouble(decodedPayload["co2"] ?? null);

                            await updateEnvironmentalInfo(log, client, sensorTwin, environmentalInfo);

                            // now, find the Exhibition Booth where this sensor is deployed
                            AsyncPageable<IncomingRelationship> rels = client.GetIncomingRelationshipsAsync(sensorTwin.Id);

                            Thread.Sleep(3000);

                            // get the sourceId (parentId)
                            string exhibitionBoothId = null;
                            BasicDigitalTwin exhibitionBooth = null;

                            await foreach (IncomingRelationship boothRel in rels)
                            {
                                if (boothRel.RelationshipName == "sensors")
                                    exhibitionBoothId = boothRel.SourceId;
                            }


                            if (exhibitionBoothId != null)
                            {
                                log.LogInformation($"Found sensor in booth {exhibitionBoothId}");

                                // get exhibition booth twin
                                AsyncPageable<BasicDigitalTwin> query2Response = client.QueryAsync<BasicDigitalTwin>($"SELECT * FROM digitaltwins WHERE $dtId = '{exhibitionBoothId}'");
                                await foreach (BasicDigitalTwin twin in query2Response)
                                {
                                    exhibitionBooth = twin;
                                }

                                // sensor belongs to a booth ==> get all sensors of the booth 
                                AsyncPageable<IDictionary<string, BasicDigitalTwin>> query3Response = client.QueryAsync<IDictionary<string, BasicDigitalTwin>>($"SELECT Sensor, ExhibitionBooth FROM digitaltwins ExhibitionBooth JOIN Sensor RELATED ExhibitionBooth.sensors WHERE ExhibitionBooth.$dtId = '{exhibitionBoothId}'");
                                List<EnvironmentalInfo> sensorList = new List<EnvironmentalInfo>();

                                // loop through each room and build a list of rooms
                                await foreach (IDictionary<string, BasicDigitalTwin> d in query3Response)
                                {
                                    if (d.ContainsKey("Sensor"))
                                    {
                                        log.LogInformation($"---{d["Sensor"]}.Id---");
                                        if (d["Sensor"].Contents.ContainsKey("environmental_info"))
                                        {
                                            JObject ss = (JObject)JsonConvert.DeserializeObject(d["Sensor"].Contents["environmental_info"].ToString());

                                            sensorList.Add(new EnvironmentalInfo()
                                            {
                                                temperature = ss["temperature"] == null ? (double?)null : Convert.ToDouble(ss["temperature"]),
                                                humidity = ss["humidity"] == null ? (double?)null : Convert.ToDouble(ss["humidity"]),
                                                co2 = ss["co2"] == null ? (double?)null : Convert.ToDouble(ss["co2"])
                                            });
                                        }
                                    }

                                   /* else if (d.ContainsKey("ExhibitionBooth"))
                                    {
                                        exhibitionBooth = d["ExhibitionBooth"];
                                    }
*/
                                }

                                // compute the averages for the sensors
                                double? avgTemperature = sensorList.Average(x => x.temperature);
                                double? avgHumidity = sensorList.Average(x => x.humidity);
                                double? avgCO2 = sensorList.Average(x => x.co2);

                                environmentalInfo = new EnvironmentalInfo()
                                {
                                    temperature = avgTemperature,
                                    humidity = avgHumidity,
                                    co2 = avgCO2
                                };

                                await updateEnvironmentalInfo(log, client, exhibitionBooth, environmentalInfo);

                            }


                            log.LogInformation("ProcessDTRoutedData (Done)...");
                            log.LogInformation(" ");


                            return;

                        }

                    }
                }
            }
        }

        private static async Task updateEnvironmentalInfo(ILogger log, DigitalTwinsClient client, BasicDigitalTwin twin, EnvironmentalInfo e)
        {
            var updateTwinData = new JsonPatchDocument();

            JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            };

            // update twin properties for the sensor
            if (twin.Contents.ContainsKey("environmental_info"))
            {
                updateTwinData.AppendReplaceRaw("/environmental_info", JsonConvert.SerializeObject(e, Formatting.None, jsonSerializerSettings));
            }
            else
            {
                updateTwinData.AppendAddRaw("/environmental_info", JsonConvert.SerializeObject(e, Formatting.None, jsonSerializerSettings));
            }

            try
            {
                log.LogInformation($"Updating twin {twin.Id}: {updateTwinData.ToString()}");
                await client.UpdateDigitalTwinAsync(twin.Id, updateTwinData);
                log.LogInformation($"Twin {twin.Id} -- UPDATED");
            }
            catch (Exception ex)
            {
                log.LogError($"Error: {ex.Message}");
            }
        }
    }
}
