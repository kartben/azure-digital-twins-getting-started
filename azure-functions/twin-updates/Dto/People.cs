using System;
using System.Collections.Generic;
using System.Text;

namespace TwinUpdatesSample.Dto
{
    class People
    {
        public string id { get; set; }
        public double body_temperature { get; set; } = Double.NaN;
        public double heart_rate { get; set; } = Double.NaN;
    }
}
