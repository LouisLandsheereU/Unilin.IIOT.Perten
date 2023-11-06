//using log4net;
using Microsoft.Extensions.Configuration;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Timers;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace Perten2MQTT
{
    /// <summary>
    /// This class holds the logic of interpreting the submitted rawdata of a perten device in production An object of this class initiates 2 timers...
    /// 1 timer that reads the published measurements of perten and copy the constantly new updated file (with the same name) to a buffer directory
    /// 1 timer that transmit data reads the buffer directoy and transmits data to a broker of MQTT
    /// After processing a file the file will / should be deleted permanently
    /// There is also an option to test the MQTT publication (parse with constructor delete file)
    /// </summary>
    public class FileDataTransmitter
    {
        #region Private members

        private System.Timers.Timer readTimer;
        private System.Timers.Timer transmitTimer;
        private System.Timers.Timer clearFilesIfNeeded;

        private string filename = "TestFile.cvs"; //hardcoded
        private string destFolder = @"c:\temp\Processing\"; 
        private DirectoryInfo destinationFolder = new DirectoryInfo(@"C:\temp\Processing\");
        private string last_tick = "";
        private MqttClient client;
        private string dtFormated = "o";
        private DateTime lastMessage = DateTime.Now;
        private long minInterval = 0;
        private string scid = "";
        private string ReadAndChangeDotByComma = "true";
        private string MQTTBroker = "";
        private int MQTTPort = 0;
        private string MQTTUser = "";
        private string MQTTPwd = "";
        private string Topic = "";
        int Read_Interval_In_ms = 10000;
        int ClearFiles_Checl_Interval_In_ms = 10000;
        int Transmit_Interval_In_ms = 10000;
        bool test = false;
        //private static readonly ILog Log =  LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        //Configuration config = null;
        private readonly IConfiguration _configuration;
        
        #endregion



        #region Constructor

        /// <summary>
        /// Constructs the filenmames and destination 
        /// Launches the timers and events
        /// </summary>
        public FileDataTransmitter(bool test)
        {
            try
            {
                //ApplicationLogger.Setup();
                //Log.Info("Construction has become");
                FillSettingsFromConfigFile();
                if (!test)
                {
                    readTimer = new System.Timers.Timer(Read_Interval_In_ms);
                    transmitTimer = new System.Timers.Timer(Transmit_Interval_In_ms);
                    clearFilesIfNeeded = new System.Timers.Timer(ClearFiles_Checl_Interval_In_ms);
                    
                    Console.WriteLine("Reading Filename & destination");
                    filename = _configuration["Filename"];
                    destFolder = _configuration["TempDestinationFolder"];

                    if (!destFolder.EndsWith(@"\"))
                    {
                        destFolder = destFolder + @"\";
                    }
                    destinationFolder = new DirectoryInfo(destFolder);

                    if (!destinationFolder.Exists)
                    {
                        destinationFolder.Create();

                    }

                    this.clearFilesIfNeeded.Elapsed += ClearFilesIfNeeded_Elapsed;
                    this.readTimer.Elapsed += readTimer_Elapsed;
                    this.transmitTimer.Elapsed += TransmitTimer_Elapsed;
                    this.clearFilesIfNeeded.Enabled = true;
                    this.readTimer.Enabled = true;
                    this.transmitTimer.Enabled = true;
                    Console.WriteLine("Contstucted the folder " + destinationFolder.ToString() + " is created");
                }
            }
            catch (Exception exception)
            {
                //Log.Error(exception.Message, exception);
                throw new Exception("Error in construction", exception);
                

            }



        }

        #endregion

        #region App.Config

        /// <summary>
        /// Fill the settings that are defined in the config file. Sections AppSettings.
        /// Some will be casted to ints
        /// </summary>
        /// <exception cref="Exception"> An exception will occour if some values can not read</exception>
        public void FillSettingsFromConfigFile()
        {

            //InitiateRightConfig;

            /*ExeConfigurationFileMap configMap = new ExeConfigurationFileMap();
            configMap.ExeConfigFilename = GetAppConfigName();
            
            config = ConfigurationManager.OpenMappedExeConfiguration(configMap, ConfigurationUserLevel.None);
            
            ConfigurationManager.RefreshSection("AppSettings");*/

            //Log.Info("The Exe config file name is " + configMap.ExeConfigFilename + " counts" + config.AppSettings.Settings.Count + " settings");

            /*foreach (string qset in varconfig.GetSection("AppSettings")
            {
                //Log.Info("The setting with key = " + qset  );
            }*/



            //scid = _configuration["Cid"];
            //  if (string.IsNullOrEmpty(scid))
              //  throw new Exception("Cid is not in appsettings");
            //Topic = _configuration["Topic"]; //Hier alles aanpassen
            //  if (string.IsNullOrEmpty(Topic))
              // throw new Exception("Topic is not in appsettings");
            //dtFormated = _configuration["DateTimeFormatUsed"];
            //  if (string.IsNullOrEmpty(dtFormated))
              //  throw new Exception("dtFormated is not in appsettings");
            //ReadAndChangeDotByComma = _configuration["ReadAndChangeDotByComma"];
            //  if (string.IsNullOrEmpty(ReadAndChangeDotByComma))
              //  throw new Exception("ReadAndChangeDotByComma is not in appsettings");
            //MQTTBroker = _configuration["MQTTBroker"];
            //  if (string.IsNullOrEmpty(MQTTBroker))
              //  throw new Exception("MQTTBroker is not in appsettings");
            /*MQTTUser = _configuration["MQTTUser"];
            if (string.IsNullOrEmpty(MQTTUser))
                throw new Exception("MQTTUser is not in appsettings");
            MQTTPwd = _configuration["MQTTPwd"];
            if (string.IsNullOrEmpty(MQTTPwd))
                throw new Exception("MQTTPwd is not in appsettings");*/

            //string sMQTTBroker = _configuration["MQTTPort"];

            //  if (string.IsNullOrEmpty(sMQTTBroker))
              //  throw new Exception("MQTTPort is not in appsettings");

            //System.Int32.TryParse(sMQTTBroker, out MQTTPort);

            int MinIntervalBetweenMessages = 1000;
            Console.WriteLine("Geen Errors");
            
            //System.Int32.TryParse(_configuration["MinIntervalBetweenMessages"], out MinIntervalBetweenMessages);

            this.minInterval = test ? 0 : MinIntervalBetweenMessages;

            //if (!System.Int32.TryParse(_configuration["Read_Interval_In_ms"], out Read_Interval_In_ms))
              //  throw new Exception("Error in reading interval setting Read_Interval_In_ms from config file");

            //if (!System.Int32.TryParse(_configuration["Transmit_Interval_In_ms"], out Transmit_Interval_In_ms))
              //  throw new Exception("Error in reading interval setting Read_Interval_In_ms from config file");

            //if (!System.Int32.TryParse(_configuration["ClearFiles_Checl_Interval_In_ms"], out ClearFiles_Checl_Interval_In_ms))
              //  throw new Exception("Error in reading interval setting Read_Interval_In_ms from config file");

        }


        public string GetAppConfigName()
        {

            string locationOdEntryAssmebly = Assembly.GetExecutingAssembly().Location;
            FileInfo fileinfoOfAssembly = new FileInfo(locationOdEntryAssmebly);


            string appconfigfilename = "app.config";
            var args = Environment.GetCommandLineArgs();
            if (args.Length > 0)
            {
                foreach (string arg in args)
                {
                    if (arg.ToLower().StartsWith("configname="))
                    {
                        appconfigfilename = arg.ToLower().Replace("configname=", "");
                        break;
                    }
                }
            }
            //Log.Info("The appconfig is " + appconfigfilename);

            appconfigfilename = fileinfoOfAssembly.Directory.FullName + @"\" + fileinfoOfAssembly.FullName.Replace(fileinfoOfAssembly.FullName, appconfigfilename);

            if (!File.Exists(appconfigfilename))
                throw new FileNotFoundException("appconfigfilename= " + appconfigfilename + "of the assembly= " + fileinfoOfAssembly.FullName, appconfigfilename);
            //Log.Info("The full appconfig is " + appconfigfilename);

            return appconfigfilename;

        }

        #endregion


        #region exceptional cases 
        /// <summary>
        /// Due to connection interuption purging files can be interupted for a while 
        /// We have to be carefull that we do not store a large ammount of data on production PC
        /// The cleea
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void ClearFilesIfNeeded_Elapsed(object sender, ElapsedEventArgs e)
        {
            Console.WriteLine("Check old file");
            //ClearOldData();

        }
        #endregion

        #region process & transmit methods

        private void TransmitTimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            //Console.WriteLine("TimerElapsed");
            try
            {
             
                Console.WriteLine("About to Transmit files to MQTT data");
                var files = destinationFolder.GetFiles();
          
                Console.WriteLine("There are " + files.Length + " to handle");

                foreach (FileInfo file in files)
                {
                    Console.WriteLine("About to handle the file");
                    try
                    {
                        ProcessFileAndTransmit(file.FullName, true);
                    }
                    catch (Exception innerexception)
                    {
                        Console.WriteLine("Processing error:" + innerexception.Message);
                    }
                    
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine(exception.Message);
            }
        }

        public void ProcessFileAndTransmit(string file, bool delete)
        {

            Dictionary<string, Dictionary<DateTime, List<SpectrumMeasurement>>> values = new Dictionary<string, Dictionary<DateTime, List<SpectrumMeasurement>>>();
            StreamReader sr = new StreamReader(file);
            string line;
            string[] row = new string[6];
            while ((line = sr.ReadLine()) != null)
            {
                try
                {
                    row = line.Split(',');


                    DateTime timestamp = (DateTime.ParseExact(row[0], dtFormated, CultureInfo.InvariantCulture));


                    var Reference = row[1];
                    var Description = row[2];
                    var Range = row[5];
                    var sValue = row[6];
                    var dValue = 0.2;


                    if (ReadAndChangeDotByComma.ToLower() == "true")
                        sValue = sValue.Replace(".", ",");

                    if (!double.TryParse(sValue, out dValue))
                    {
                        Console.WriteLine("Could not parse the value");
                    }

                    if (!values.ContainsKey(Reference))
                        values.Add(Reference, new Dictionary<DateTime, List<SpectrumMeasurement>>());


                    if (!values[Reference].ContainsKey(timestamp))
                        values[Reference].Add(timestamp, new List<SpectrumMeasurement>());

                    values[Reference][timestamp].Add(new SpectrumMeasurement(Reference, Range, dValue)); ;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Parsing error:" + ex.Message);
                }
            }
            sr.Close();

            Transmit(values);

            if (delete)
            {
                Console.WriteLine("About to delete files ");
                if (File.Exists(file))
                {
                    File.Delete(file);
                    Console.WriteLine("deleted file ");
                }
            }
        }

        #endregion

        #region read methods

        private void readTimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            try
            {
                Console.WriteLine("Read timer fired");
                if (File.Exists(filename))
                {

                    DateTime lastwrite = File.GetLastWriteTime(filename);
                    var new_last_tick = lastwrite.Ticks.ToString();
                    
                    if (new_last_tick == last_tick)
                    {
                        //Nothing todo last file is processed
                        return;
                    }


                    var new_destination_file = destFolder + new_last_tick + ".csv";

                    var sourceFile = new FileInfo(filename);
                    sourceFile.CopyTo(new_destination_file, true);
                    
                    last_tick = new_last_tick;

                }

            }
            catch (Exception exception)
            {
                //logger 
                Console.WriteLine(exception.Message);
                
            }
        }

        #endregion

        #region MQTT Jobs

        private void EstablishConnectionWithBroker()
        {
            //Reading Broker

            if (client == null)
            {


                client = new MqttClient(MQTTBroker, MQTTPort, true, MqttSslProtocols.TLSv1_2);
            }
            if (!client.IsConnected)
            {

                client.ProtocolVersion = MqttProtocolVersion.Version_3_1;
                client.Connect(Guid.NewGuid().ToString(), MQTTUser, MQTTPwd);
                var cl = client.CleanSession;
                client.ConnectionClosed += Client_ConnectionClosed;

            }

        }

        private void Client_ConnectionClosed(object sender, EventArgs e)
        {
            //Connection closed
        }

        public void Transmit(Dictionary<string, Dictionary<DateTime, List<SpectrumMeasurement>>> spectrumDataForMQTTPublication)
        {
            EstablishConnectionWithBroker();

            string strmsg = "{";

            foreach (string device in spectrumDataForMQTTPublication.Keys)
            {

                foreach (DateTime timestamp in spectrumDataForMQTTPublication[device].Keys)
                {
                    DateTime utimestamp = timestamp.ToUniversalTime();

                    var stime = utimestamp.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'");

                    strmsg = strmsg + "\"timestamp\":\"" + stime + "\",\"cid\":\"" + scid + "\"";
                    foreach (SpectrumMeasurement spm in spectrumDataForMQTTPublication[device][timestamp])
                    {
                        strmsg = strmsg + ",\"Range_" + spm.Range + "\":" + spm.Value.ToString().Replace(",", ".");
                    }
                    strmsg = strmsg + "}";
                    var bytearray = Encoding.ASCII.GetBytes(strmsg);
                    var QoS = MqttMsgConnect.QOS_LEVEL_EXACTLY_ONCE;

                    TimeSpan tspn = (lastMessage - timestamp);
                    if ((Math.Abs(Convert.ToDouble(tspn.TotalMilliseconds))) > Convert.ToDouble(minInterval))
                    {
                        lastMessage = timestamp;

                        Console.WriteLine("About to publish:");
                        Console.WriteLine(strmsg);
                        client.Publish(Topic, bytearray, QoS, false);

                    }
                    else
                    {
                        Console.WriteLine("About not to publish:");
                    }
                }
            }

            //Only publish is the min interval is reached


        }

        #endregion

        #region Configuration

        public FileDataTransmitter(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        #endregion

    }
}



