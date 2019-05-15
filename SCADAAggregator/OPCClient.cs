using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Timers;
using System.Xml;
using System.Text;
using System.Threading.Tasks;
using TitaniumAS.Opc.Client;
using TitaniumAS.Opc.Client.Da;
using Common.Logging;
using System.Threading;
using System.IO;
using CsvHelper;
using System.Data.SqlClient;
using System.Configuration.Install;

namespace SCADAAggregator
{
    [RunInstaller(true)]
    public partial class AggregateServiceInstaller : Installer
    {
        public AggregateServiceInstaller()
        {
            //Instantiate and configure a ServiceProcessInstaller
            ServiceProcessInstaller PollingService = new ServiceProcessInstaller();
            PollingService.Account = ServiceAccount.LocalSystem;

            //Instantiate and configure a ServiceInstaller
            ServiceInstaller SCADAInstaller = new ServiceInstaller();
            SCADAInstaller.DisplayName = "GAMESA SCADA Aggregator";
            SCADAInstaller.ServiceName = "GAMESA SCADA Aggregator";
            SCADAInstaller.StartType = ServiceStartMode.Automatic;

            //Add both the service process installer and the service installer to the
            //Installers collection, which is inherited from the Installer base class.
            Installers.Add(SCADAInstaller);
            Installers.Add(PollingService);
        }
    }

    public class ClientTag
    {
        public ClientTag(string newItem, AggregatorTag newTag)
        {
            CollectedValues = new List<OpcDaItemValue>();
            OPCTag = newItem;
            //Scaling = newTag.Scaling;
            WBI_Columns = new List<string>(newTag.WBI_Columns);
            //WBI_Columns.Add(newTag.WBI_Column);
            Transforms = new List<string>(newTag.Transforms);
            //Transforms.Add(newTag.Transform);
            bAdded = false;
        }
        public List<OpcDaItemValue> CollectedValues;
        public bool bAdded { get; set; }
        protected OpcDaItem myItem;
        public string OPCTag { get; set; }
        //public string Scaling { get; set; }
        public List<string> WBI_Columns { get; set; }
        public List<string> Transforms { get; set; }
        
    }

    public class CalcColumn
    {
        public CalcColumn(string szValue, string szColumnName)
        {
            CalcValue = szValue;
            ColumnName = szColumnName;
        }
        public string CalcValue { get; set; }
        public string ColumnName { get; set; }
    }

    public class OPCClient
    {
        
        public string ProgID { get; set; }
        public string Server_address { get; set; }
        public int UpdateRate;
        //public bool connected = false;
        //public DateTime lastUpdated;
        //public DateTime ExecuteTime;
        //public string Action;
        //public string ItemID;
        //public object Value;
        //public TimeSpan StaleTimeout;
        //public TimeSpan WaitTimeout;
        //public int connectTimeout;
        public List<AggregatorTag> opc_tags;
        public List<ClientTag> Client_Tags;
        //public List<AggregatorTag> opc_met_tags;
        public List<FaultCode> ls_FaultCodes;
        public List<string> turbine_list;
        //public List<string> met_list;
        OpcDaServer myServer;
        public List<OpcDaGroup> DAGroups;
        public bool bScanning;
        protected System.Timers.Timer ScanningTimer;
        protected System.Timers.Timer SQLTimer;


        private int ScanningRate = 0;
        private int SQLUpdateRate = 0;
        private string SQLServer = "";
        private string SQLUser = "";
        private string SQLPass = "";
        private string SQLDB = "";
        private string SQLTable = "";
        private string SQLCatalog = "";

        public OPCClient(string newProgID, int newScanningRate, int newSQLUpdateRate, string newSQLServer, string newSQLUser, string newSQLPass, string newSQLDB, string newSQLTable, string newSQLCatalog)
        {
            opc_tags = new List<AggregatorTag>();
            //opc_met_tags = new List<AggregatorTag>();
            ls_FaultCodes = new List<FaultCode>();
            turbine_list = new List<string>();
            DAGroups = new List<OpcDaGroup>();
            Client_Tags = new List<ClientTag>();
            //met_list = new List<string>();
            this.ProgID = newProgID;
            bScanning = true;
            //Log = new ILog();
            ScanningRate = newScanningRate;
            SQLUpdateRate = newSQLUpdateRate;
            SQLServer = newSQLServer;
            SQLUser = newSQLUser;
            SQLPass = newSQLPass;
            SQLDB = newSQLDB;
            SQLTable = newSQLTable;
            SQLCatalog = newSQLCatalog;

        }

        public void InitializeComponent()
        {
            ScanningTimer = new System.Timers.Timer();
            ScanningTimer.Enabled = false;
            ScanningTimer.Interval = 10000;
            ScanningTimer.Elapsed += OnTimedScanTime;
            ScanningTimer.AutoReset = false;
            SQLTimer = new System.Timers.Timer();
            SQLTimer.Enabled = false;
            // MWH SQL Timer should be 600000
            SQLTimer.Interval = 600000;
            SQLTimer.Elapsed += OnTimedSQLPush;
            SQLTimer.AutoReset = false;
            Console.WriteLine("Scanning Interval {0} SQL Interval {0}",
                  ScanningTimer.Interval, SQLTimer.Interval);
        }

        public bool Equals(string ProgID)
        {
            if (ProgID == null) return false;
            return (this.ProgID.Equals(ProgID));
        }

        public void AddTurbine(string newTurbine)
        {
            turbine_list.Add(newTurbine);
        }

        //public void AddMET(string newMet)
        //{
        //    met_list.Add(newMet);
        //}

        public void AddTag(AggregatorTag newTag)
        {
            AggregatorTag myTag = null;
            myTag = opc_tags.Find(x => x.OPCTag.Contains(newTag.OPCTag));
            if (myTag != null)
            {
                myTag.Transforms.Add(newTag.Transform);
                myTag.WBI_Columns.Add(newTag.WBI_Column);
            }
            else
            {
                newTag.Transforms.Add(newTag.Transform);
                newTag.WBI_Columns.Add(newTag.WBI_Column);
                opc_tags.Add(newTag);
                //Client_Tags.Add();
                //iIndex = curTag.OPCTag.IndexOf('%');
                //trueTagname = curTag.OPCTag.Substring(0, iIndex) + curTurbine + curTag.OPCTag.Substring(iIndex + 1);

                //newTag = new ClientTag(trueTagname, curTag);
                ////curTag.SetTagname(trueTagname);
                //Client_Tags.Add(newTag);
            }

            //Console.WriteLine("\nFind: Part where name contains \"seat\": {0}",
            //parts.Find(x => x.PartName.Contains("seat")));
            //AggregatorTag newTag;
            //newTag.Tagname = Tagname;
            //newTag.Transform = Transform;
            //newTag.WBI_Column = WBI_Column;
            //newTag.SQL_Format = SQL_Format;
            //newTag.scaling = Scaling;
            //opc_tags.Add(newTag);
        }

        //public void AddMetTag(AggregatorTag newTag)
        //{
        //    opc_met_tags.Add(newTag);
        //}

        public ILog Log { get; private set; }
        //        using System.Threading;

        internal bool IsConnected()
        {
            bool bConnected = false;
            try
            {
                bConnected = myServer.IsConnected;
            }
            catch (Exception)
            {
                bConnected = false;
            }
            return bConnected;
        }

        internal bool Connect()
        {
            bool bIsConnected = false;
            try
            {
                myServer = new OpcDaServer(ProgID);
                myServer.ClientName = "SCADA_Aggregator.exe";
            }
            catch (Exception)
            {
                return false;
            }

            StartConnect();
            int i;
            for (i = 0; i < 10; i++)
            {
                try
                {
                    bIsConnected = myServer.IsConnected;
                    if (bIsConnected)
                        break;
                }
                catch (Exception)
                {

                }

                Thread.Sleep(Convert.ToInt32(1000));
            }
            if (i == 10)
            {
                Log.Error("Could not connect to OPC Server: [" + ProgID + "] Check to see that the OPC Server is running or check your DCOM settings.");
            }
            //isConnected = myServer.IsConnected;
            if (bIsConnected == true)
            {
                //Value = -1231231212314.12312323; // This is a random negative number to iniatialize the value to so that we do not compare this when we read the first value.
            }

            //lastUpdated = DateTime.MinValue;
            //ExecuteTime = DateTime.MinValue;
            return bIsConnected;
        }

        // Start the OPC Server in a seperate thread just incase it locks up when connecting.
        private void StartConnect()
        {

            BackgroundWorker bw = new BackgroundWorker();

            // this allows our worker to report progress during work
            bw.WorkerReportsProgress = true;

            //Log.Info("Connecting to OPC Server [" + ProgID + "]");
            // what to do in the background thread
            bw.DoWork += new DoWorkEventHandler(
            delegate (object o, DoWorkEventArgs args)
            {
                BackgroundWorker b = o as BackgroundWorker;
                try
                {

                    myServer.Connect();
                }
                catch (Exception)
                {
                }
            });

            //Log.Info("Now connected to OPC Server [" + ProgID + "]");
            bw.RunWorkerAsync();
        }

        internal bool AddItems(OpcDaGroup currentGroup, string curTurbine)
        {
            bool bItemAdded = false;

            //var definition = new OpcDaItemDefinition;
            string trueTagname;
            OpcDaItemDefinition[] definitions = new OpcDaItemDefinition[opc_tags.Count];
            int iCount = 0;
            int iIndex = 0;
            ClientTag newTag;
            foreach (AggregatorTag curTag in opc_tags)
            {
                iIndex = curTag.OPCTag.IndexOf('%');
                trueTagname = curTag.OPCTag.Substring(0, iIndex) + curTurbine + curTag.OPCTag.Substring(iIndex+1);

                newTag = new ClientTag(trueTagname, curTag);
                //curTag.SetTagname(trueTagname);
                Client_Tags.Add(newTag);
                definitions[iCount] = new OpcDaItemDefinition();
                definitions[iCount].ItemId = trueTagname;
                definitions[iCount].IsActive = true;
                //definition = new OpcDaItemDefinition;
                iCount++;
            }

            //OpcDaItemDefinition[] definitions = { definition };
            OpcDaItemResult[] results = currentGroup.AddItems(definitions);

            // Handle adding results.
            bItemAdded = true;
            foreach (OpcDaItemResult result in results)
            {
                if (result.Error.Failed)
                {
                    Console.WriteLine("Error adding items: {0}", result.Error);
                    //Log.Error("Error adding item: [" + ItemID + "] Error: [" + result.Error.ToString() + "]");
                    //bItemAdded = false;
                    try
                    {
                        // Add this item to a bad items list to be retried
                        //Disconnect();
                    }
                    catch (Exception)
                    {

                    }
                }
                else
                {
                    ClientTag isFound = Client_Tags.Find(x => x.OPCTag.Contains(result.Item.ItemId));
                    if(isFound != null)
                    {
                        isFound.bAdded = true;
                    }
                    //Log.Info("Item [" + ItemID + "] Added successfully");
                }
            }

            return bItemAdded;
        }

        static void OnGroupValuesChanged(object sender, OpcDaItemValuesChangedEventArgs args)
        {
            // Output values.
            foreach (OpcDaItemValue value in args.Values)
            {
                Console.WriteLine("ItemId: {0}; Value: {1}; Quality: {2}; Timestamp: {3}",
                    value.Item.ItemId, value.Value, value.Quality, value.Timestamp);
            }
        }

        internal bool ReadValues()
        {
            bool bReadSuccessful = false;
            bool bConnected = false;
            //Log.Info("Reading Values");

            // Read all items of the group synchronously.
            try
            {
                bConnected = myServer.IsConnected;
            }
            catch (Exception)
            {
                bConnected = false;
            }
            if (bConnected == true)
            {
                OpcDaItemValue[] values;
                //myServer.Groups
                foreach (OpcDaGroup myGroup in myServer.Groups)
                {
                    try
                    {
                        //Log.Info("Sending Sync Read");
                        Console.WriteLine("Sending Sync Read");
                        values = myGroup.Read(myGroup.Items, OpcDaDataSource.Device);

                    }
                    catch (Exception)
                    {
                        //Log.Info("Exception while reading values");

                        return false;
                    }
                    ClientTag myTag = null;
                    foreach (OpcDaItemValue value in values)
                    {
                        object tempValue;
                        //value.Value;
                        try
                        {
                            tempValue = Convert.ToString(value.Value);
                        }
                        catch (Exception)
                        {
                            tempValue = -999;
                        }
                        //Console.WriteLine("Evaluating OPC Item [" + value.Item.ItemId + "] read successfully. Value: [" + tempValue + "] Quality: [" + Convert.ToDouble(value.Quality.Status) + "] Timestamp set to [" + value.Timestamp.ToString() + "]");
                        if ((value.Quality.Status & OpcDaQualityStatus.Good) == OpcDaQualityStatus.Good)// || lastUpdated == DateTime.MinValue)
                        {
                            //AggregatorTag myTag = null;
                            myTag = Client_Tags.Find(x => x.OPCTag.Contains(value.Item.ItemId));
                            if(myTag != null)
                            {
                                //Console.WriteLine("Item found and adding to collected Values");
                                myTag.CollectedValues.Add(value);
                            }
                            //Log.Info("OPC Item [" + ItemID + "] read successfully. Value: [" + tempValue + "] Quality: [" + value.Quality.Status.ToString() + "] Timestamp set to [" + DateTime.Now + "]");
                            //if (tempValue != Value)
                            //{
                            //    Value = tempValue;
                            //    lastUpdated = DateTime.Now;
                            //    Log.Info("OPC Item [" + ItemID + "] has changed. Updating stored value and timestamp");
                            //}
                            //else
                            //{
                            //    Log.Info("OPC Item [" + ItemID + "] has NOT changed. Ignoring read");
                            //}

                        }
                        else
                        {
                            Console.WriteLine("OPC Item [" + value.Item.ItemId + "] has come back with BAD quality. This does NOT count as a valid update");
                            //Log.Warn("OPC Item [" + ItemID + "] has come back with BAD quality. This does NOT count as a valid update");
                        }
                        bReadSuccessful = true;
                        //Console.WriteLine("Item: " + value.Item.ItemId.ToString() + " Value: " + value.Value.ToString());
                    }

                }

                //Log.Info("Returned [" + values.Count().ToString() + "] values from read");
                
            }
            return bReadSuccessful;
        }

        public void Disconnect()
        {
            bool bConnected = false;
            Log.Warn("Disconnecting from OPC Server [" + ProgID + "]");

            try
            {
                bConnected = myServer.IsConnected;
            }
            catch (Exception)
            {
                bConnected = false;
            }
            if (bConnected)
            {
                try
                {
                    //myServer.RemoveGroup(myGroup);
                    myServer.Disconnect();
                }
                catch (Exception)
                {
                }
            }
            //connected = false;
            myServer = null;
            //lastUpdated = DateTime.MinValue;
        }



        internal void SetLogging(ILog log)
        {
            Log = log;
        }

        internal void AddFaultCode(FaultCode tag)
        {
            ls_FaultCodes.Add(tag);
            //throw new NotImplementedException();
        }

        internal void Scan()
        {
            //DateTime nowTime = DateTime.Now;
            //Debug.Print("Seconds is {0}", nowTime.Second);
            //if (nowTime.Second % (ScanningRate / 1000) == 0)
            {
                //ScanningTimer.Enabled = true;
                ScanningTimer.Interval = GetScanningInterval();
                ScanningTimer.Start();
                Debug.Print("Enabling Scan Scanning Timer");

            }
            //nowTime = DateTime.Now;
            //Debug.Print("Minutes is {0}", nowTime.Minute);
            //if (nowTime.Minute % 10 == 0)
            //if (nowTime.Second % (ScanningRate / 1000) == 0)
            {
                SQLTimer.Interval = GetSQLScanningInterval();
                SQLTimer.Start();
                Debug.Print("Enabling SQL Scanning Timer");

            }

            while (bScanning)
            {
                // if not connected then connect and add tags
                if (IsConnected() == false)
                {
                    if (Connect() == true)
                    {
                        AddOPCItems();

                    }

                }
                // if not connected to SQL then connect
                //if (IsSQLConnected() == false)
                {
                    //if (SQLConnect() == true)
                    {
                        // Setup SQL Query

                    }

                }
                // Add items that were not added succesfully?

                // Setup Timer Synchronization
                //if( ScanningTimer.Enabled == false)
                //{
                //    DateTime nowTime = DateTime.Now;
                //    //Debug.Print("Seconds is {0}", nowTime.Second);
                //    if (nowTime.Second % (ScanningRate / 1000) == 0)
                //    {
                //        //ScanningTimer.Enabled = true;
                //        ScanningTimer.Start();
                //        Debug.Print("Enabling Scan Scanning Timer");
                        
                //    }
                //}

                //if (SQLTimer.Enabled == false)
                //{
                //    DateTime nowTime = DateTime.Now;
                //    //Debug.Print("Minutes is {0}", nowTime.Minute);
                //    //if (nowTime.Minute % 10 == 0)
                //    if (nowTime.Second % (ScanningRate / 1000) == 0)
                //    {
                //        SQLTimer.Interval = GetSQLScanningInterval();
                //        SQLTimer.Start();
                //        Debug.Print("Enabling SQL Scanning Timer");

                //    }
                //}
                // Start collection of values for aggregation
                // Every 10 second read items and add to collection
                //
                // Is it the top of 10 minutes? then send out SQL message
                Thread.Sleep(10);
            }
            // Disconnect from OPC and SQL

            //throw new NotImplementedException();
        }

        private void AddOPCItems()
        {
            int iGroupCount = 0;
            string szGroupName;
            OpcDaGroup newGroup;
            TimeSpan UpdateRate = new TimeSpan(0,0,0, (ScanningRate / 1000));
            //OpcDaGroupState GroupState;
            foreach (string curTurbine in turbine_list)
            {
                szGroupName = "AggregatorGroup_" + iGroupCount.ToString();
                newGroup = myServer.AddGroup(szGroupName);
                if (newGroup != null)
                {
                    newGroup.UpdateRate = UpdateRate;
                    AddItems(newGroup, curTurbine);
                }
                //myGroup = myServer.AddGroup("AutoRestartGroup");
                iGroupCount++;
            }
            //throw new NotImplementedException();
        }

        private void OnTimedScanTime(Object source, ElapsedEventArgs e)
        {
            ReadValues();

            // Re-adjust the timer to ensure we run on at the 10 second mark
            Console.WriteLine("The Elapsed Scanned event was raised at {0:HH:mm:ss.fff}",
                  e.SignalTime);
            ScanningTimer.Interval = GetScanningInterval();
            ScanningTimer.Start();
           
        }

        // Get how long it is to execute closest to the next scan interval.
        private double GetScanningInterval()
        {
            DateTime now = DateTime.Now;
            double iMilliseconds = (((ScanningRate / 1000) - (now.Second % 10)) * 1000 - now.Millisecond);

            Console.WriteLine("Scanning Interval {0}",
                  iMilliseconds);
            return iMilliseconds;
        }


        // Get how long it is to execute closest to the next SQL Push time.
        private double GetSQLScanningInterval()
        {
            DateTime now = DateTime.Now;
            // total scan time  - (Minutes % 10 - seconds - milliseconds)
            //double iMilliseconds = (((SQLUpdateRate / 1000) - (now.Second % 10)) * 1000 - now.Millisecond);
            // 300 000 - 3 
            double iMilliseconds = SQLUpdateRate - (((now.Minute % (SQLUpdateRate / 60000)) * 60000) + (now.Second * 1000) + now.Millisecond);

            Console.WriteLine("SQL Scanning Interval {0}",
                  iMilliseconds);
            Console.WriteLine("SQLUpdateRate {0} - (Now.Minute {1} % {2} + ((now.Second % 10) * 1000) {3} + Millisecond {4} : CurrentTime {5:HH:mm:ss.fff}",
                    SQLUpdateRate, now.Minute, SQLUpdateRate / 60000, ((now.Second % 10) * 1000), now.Millisecond, now);
            return iMilliseconds;
        }

        private void OnTimedSQLPush(Object source, ElapsedEventArgs e)
        {
            Console.WriteLine("The Elapsed SQL event was raised at {0:HH:mm:ss.fff}",
                              e.SignalTime);
            SQLConnect();

            SQLTimer.Interval = GetSQLScanningInterval();
            SQLTimer.Start();
        }

        private bool SQLConnect()
        {
            //SqlConnection SQLConn = new SqlConnection("Data Source=(local)\\SQLEXPRESS;Initial Catalog=WCS;Integrated Security=SSPI");
            string szConnectionString = "Data Source=(local)\\SQLEXPRESS;Initial Catalog=WCS;Integrated Security=SSPI";
            //SqlDataReader rdr = null;
            try
            {
                string sqlCmd;
                //SQLConn.Open();
                foreach (string szTurbine in turbine_list)
                {
                    sqlCmd = BuildSQLCommand(szConnectionString, szTurbine);
                }
                //SqlCommand cmd = new SqlCommand(sqlCmd, SQLConn);
                //rdr = cmd.ExecuteReader();
                //while(rdr.Read())
                //{
                //    Console.WriteLine(rdr[1]);
                //}
            }
            finally
            {
                //if(rdr != null)
                //{
                //    rdr.Close();
                //}
                //if (SQLConn != null)
                //{
                //    SQLConn.Close();
                //}
                foreach(ClientTag myTag in Client_Tags)
                {
                    myTag.CollectedValues.Clear();
                }
            }
            //throw new NotImplementedException();
            return true;
        }

        private string BuildSQLCommand(string szConnectionString, string szTurbine) //SqlConnection SQLConn, string szTurbine)
        {

            string szNewConnection;
            szNewConnection = "Data Source=" + SQLServer + ";Initial Catalog=" + SQLCatalog + ";Integrated Security=SSPI;User ID=" + SQLUser + ";Password=" + SQLPass + ";";

            //SqlConnection SQLConn = new SqlConnection("Data Source=(local)\\SQLEXPRESS;Initial Catalog=WCS;Integrated Security=SSPI");
            SqlConnection SQLConn = new SqlConnection(szNewConnection);
            string sqlCommand = "";
            string szTransform;
            string szColumn;
            string szResult;

            string[] transformarray;
            string[] columnsArray;
            CalcColumn ResultHolder;

            try
            { 
                SQLConn.Open();
                List<CalcColumn> ResultList = new List<CalcColumn>();
            
                ResultHolder = new CalcColumn(System.DateTime.Now.ToString(), "date_time_stamp");
                ResultList.Add(ResultHolder);
                ResultHolder = new CalcColumn(szTurbine, "device_id");
                ResultList.Add(ResultHolder);

                foreach (ClientTag curTag in Client_Tags)
                {
                    if (curTag.OPCTag.Contains(szTurbine))
                    {
                        transformarray = curTag.Transforms.ToArray();
                        columnsArray = curTag.WBI_Columns.ToArray();
                        for (int i = 0; i < transformarray.Count(); i++)
                        {
                            //curTag.CollectedValues;
                            szTransform = transformarray[i];
                            szColumn = columnsArray[i];
                            szResult = DoCalculations(szTransform, curTag.CollectedValues);
                            ResultHolder = new CalcColumn(szResult, szColumn);

                            ResultList.Add(ResultHolder);

                        }
                    }
                }

                string SQLCommandString = "Insert into dbo.[" + SQLTable + "] (";
                int iCount = 1;
                foreach(CalcColumn curCalc in ResultList)
                {
                    SQLCommandString += curCalc.ColumnName;
                    if(iCount != ResultList.Count())
                    {
                        SQLCommandString += ",";
                    }
                
                    iCount++;
                }
                iCount = 1;
                SQLCommandString += ") VALUES (";
                foreach (CalcColumn curCalc in ResultList)
                {
                    SQLCommandString += "@" + curCalc.ColumnName;
                    if (iCount != ResultList.Count())
                    {
                        SQLCommandString += ",";
                    }

                    iCount++;
                }
                SQLCommandString += ")";

                SqlCommand command = new SqlCommand(SQLCommandString, SQLConn);

                foreach (CalcColumn curCalc in ResultList)
                {
                    command.Parameters.AddWithValue("@" + curCalc.ColumnName, curCalc.CalcValue);
                    Console.WriteLine("ColumnName {0} Value {1}", curCalc.ColumnName, curCalc.CalcValue);
                }

                //SQLConn.Open();
                Console.WriteLine(SQLCommandString);
                int result = command.ExecuteNonQuery();
                

                // Check Error
                if (result < 0)
                    Console.WriteLine("Error inserting data into Database!");
            }
            finally
            {
                if (SQLConn != null)
                    SQLConn.Close();
            }

            return sqlCommand;
        }

        private string DoCalculations(string szTransform, List<OpcDaItemValue> valueCollection)
        {
            string szResult = "";
            double dValue = -99112233;
            List<double> valList;

            switch (szTransform.ToUpper())
            {
                case "MAX":
                    foreach(OpcDaItemValue val in valueCollection)
                    {
                        if(dValue < Convert.ToDouble(val.Value) || dValue == -99112233)
                        {
                            dValue = Convert.ToDouble(val.Value);
                        }
                    }
                    break;
                case "MIN":
                    foreach (OpcDaItemValue val in valueCollection)
                    {
                        if (dValue > Convert.ToDouble(val.Value) || dValue == -99112233)
                        {
                            dValue = Convert.ToDouble(val.Value);
                        }
                    }
                    break;
                case "MEAN":
                    valList = new List<double>();
                    foreach (OpcDaItemValue val in valueCollection)
                    {
                        valList.Add(Convert.ToDouble(val.Value));
                    }
                    if (valList.Count > 0)
                        dValue = valList.Average();
                    else
                        dValue = 0;
                    break;
                case "STDDEV":
                    valList = new List<double>();
                    foreach (OpcDaItemValue val in valueCollection)
                    {
                        valList.Add(Convert.ToDouble(val.Value));
                    }
                    if (valList.Count > 0)
                        dValue = CalculateStdDev(valList);
                    else
                        dValue = 0;
                    break;
                case "LAST":
                default:
                    foreach (OpcDaItemValue val in valueCollection)
                    {
                        dValue = Convert.ToDouble(val.Value);
                        
                    }
                    break;
                    
            }

            if (dValue == -99112233)
                szResult = "";
            else
                szResult = dValue.ToString();
            return szResult;
        }

        private double CalculateStdDev(IEnumerable<double> values)
        {
            double ret = 0;
            if (values.Count() > 0)
            {
                //Compute the Average      
                double avg = values.Average();
                //Perform the Sum of (value-avg)_2_2      
                double sum = values.Sum(d => Math.Pow(d - avg, 2));
                //Put it all together      
                ret = Math.Sqrt((sum) / (values.Count() - 1));
            }
            return ret;
        }

        private bool IsSQLConnected()
        {
            throw new NotImplementedException();
        }
    
}
}
