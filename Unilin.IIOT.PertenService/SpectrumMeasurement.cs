using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Perten2MQTT;

public class SpectrumMeasurement
{
    public string DeviceNr;
    public string Range;
    public double Value;
    public SpectrumMeasurement(string device, string range, double val)
    {
        this.DeviceNr = device;
        this.Range = range;
        this.Value = val;
    }
}
