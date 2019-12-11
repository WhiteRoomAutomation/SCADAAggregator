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

namespace OPCScanner
{
    [RunInstaller(true)]
    public partial class ScannerServiceInstaller : Installer
    {
        public ScannerServiceInstaller()
        {
            //Instantiate and configure a ServiceProcessInstaller
            ServiceProcessInstaller PollingService = new ServiceProcessInstaller();
            PollingService.Account = ServiceAccount.LocalSystem;

            //Instantiate and configure a ServiceInstaller
            ServiceInstaller SCADAInstaller = new ServiceInstaller();
            SCADAInstaller.DisplayName = "WRA OPC Tag Scanner";
            SCADAInstaller.ServiceName = "WRA OPC Tag Scanner";
            SCADAInstaller.StartType = ServiceStartMode.Automatic;

            //Add both the service process installer and the service installer to the
            //Installers collection, which is inherited from the Installer base class.
            Installers.Add(SCADAInstaller);
            Installers.Add(PollingService);
        }
    }

    public class ClientTag
    {
        public ClientTag(string newItem, ScanningTag newTag)
        {
            CollectedValues = new List<OpcDaItemValue>();
            OPCTag = newItem;
            //WBI_Columns = new List<string>(newTag.WBI_Columns);
            //Transforms = new List<string>(newTag.Transforms);
            bAdded = false;
            failCount = 0;
            successCount = 0;
            //if (newTag.Critical.ToUpper() == "TRUE")
            //    bCritical = true;
            //else
            //    bCritical = false;
        }
        public List<OpcDaItemValue> CollectedValues;
        public bool bAdded { get; set; }
        protected OpcDaItem myItem;
        public string OPCTag { get; set; }
        public List<string> WBI_Columns { get; set; }
        public List<string> Transforms { get; set; }
        public int failCount { get; set; }
        public int successCount { get; set; }
        public bool bCritical { get; set; }

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

    public class Turbine
    {
        public string Name { get; set; }
        public int GoodReads { get; set; }
        public int BadReads { get; set; }

        internal void AddGoodRead(bool bGoodRead)
        {
            if(bGoodRead)
            {
                GoodReads = GoodReads + 1;
            }
            else
            {
                BadReads = BadReads + 1;
            }
        }
        internal void ClearReads()
        {
            GoodReads = 0;
            BadReads = 0;
        }
    }
    public class OPCClient
    {
        
        public string ProgID { get; set; }
        public string Server_address { get; set; }
        public int UpdateRate;
        public List<OpcDaItem> opc_tags;
        //public List<FaultTag> fault_tags;
        public List<ScanningTag> Client_Tags;
        //public List<FaultCode> ls_FaultCodes;
        public List<Turbine> turbine_list;
        OpcDaServer myServer;
        public bool bScanning;
        protected System.Timers.Timer ScanningTimer;
        protected System.Timers.Timer SQLTimer;
        private static log4net.ILog Log { get; set; }


        private int ScanningRate = 0;

        private string Hostname = null;

        public OPCClient(string newProgID, string newHostName, int newScanningRate, log4net.ILog newLog)
        {
            //opc_tags = new List<AggregatorTag>();
            //fault_tags = new List<FaultTag>();
            //ls_FaultCodes = new List<FaultCode>();
            //turbine_list = new List<Turbine>();
            opc_tags = new List<OpcDaItem>();
            Client_Tags = new List<ScanningTag>();
            this.ProgID = newProgID;
            Hostname = newHostName;
            bScanning = true;
            ScanningRate = newScanningRate;

            Log = newLog;

        }

        public void InitializeComponent()
        {
            ScanningTimer = new System.Timers.Timer();
            ScanningTimer.Enabled = false;
            ScanningTimer.Interval = 10000;
            ScanningTimer.Elapsed += OnTimedScanTime;
            ScanningTimer.AutoReset = false;
            //SQLTimer = new System.Timers.Timer();
            //SQLTimer.Enabled = false;
            //// SQL Timer should be 600000 for 10 minutes
            //SQLTimer.Interval = 600000;
            //SQLTimer.Elapsed += OnTimedSQLPush;
            //SQLTimer.AutoReset = false;
            //string szMessage = "Scanning Interval {" + ScanningTimer.Interval.ToString() + "} SQL Interval {" + SQLTimer.Interval.ToString() + "}";
            //Log.Info(szMessage);
        }

        public bool Equals(string ProgID)
        {
            if (ProgID == null) return false;
            return (this.ProgID.Equals(ProgID));
        }

        //public void AddTurbine(string TurbineName)
        //{
        //    Turbine newTurbine = new Turbine();
        //    newTurbine.Name = TurbineName;
        //    newTurbine.GoodReads = 0;
        //    newTurbine.BadReads = 0;
        //    turbine_list.Add(newTurbine);
        //}

        //public void AddTag(FaultTag newTag)
        //{

        //        fault_tags.Add(newTag);

        //}

        public void AddTag(ScanningTag newTag)
        {
            ScanningTag myTag = null;
            myTag = Client_Tags.Find(x => x.OPC_Tagname.Contains(newTag.OPC_Tagname));
            if (myTag != null)
            {
                //myTag.Transforms.Add(newTag.Transform);
                //myTag.WBI_Columns.Add(newTag.SQL_Column);
            }
            else
            {
                //newTag.Transforms.Add(newTag.Transform);
                //newTag.WBI_Columns.Add(newTag.SQL_Column);
                Client_Tags.Add(newTag);

            }

        }

        internal void ActivateGroups()
        {
            foreach(OpcDaGroup curGroup in myServer.Groups)
            {
                curGroup.IsActive = true;
            }
        }

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
            
                myServer = new OpcDaServer(ProgID, Hostname);
                myServer.ClientName = "OPCScanner.exe";
            }
            catch (Exception)
            {
                return false;
            }

            StartConnect();
            int i;
            // try to connect once every second for 10 seconds. If this fails then return a connection error.
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
                if(Hostname == null)
                    Log.Error("Could not connect to OPC Server: [" + ProgID + "] Check to see that the OPC Server is running or check your DCOM settings.");
                else
                    Log.Error("Could not connect to OPC Server: [" + ProgID + "] Host [" + Hostname + "] Check to see that the OPC Server is running or check your DCOM settings.");
            }

            return bIsConnected;
        }

        // Start the OPC Server in a seperate thread just incase it locks up when connecting.
        private void StartConnect()
        {

            BackgroundWorker bw = new BackgroundWorker();

            // this allows our worker to report progress during work
            bw.WorkerReportsProgress = true;

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

            bw.RunWorkerAsync();
        }

        internal bool AddItems(OpcDaGroup currentGroup, int iItemStart, int iItemEnd, List<ScanningTag> myTags)
        {
            bool bItemAdded = false;

            //string trueTagname;
            OpcDaItemDefinition[] definitions = new OpcDaItemDefinition[iItemEnd - iItemStart];
            int iCount = 0;
            //ClientTag newTag;
            for (int iIndex = iItemStart; iIndex < iItemEnd; iIndex++)
            {
             
                //trueTagname = curTag.OPC_Tagname.Substring(0, iIndex) + curTurbine + curTag.OPC_Tagname.Substring(iIndex + 1);

                //newTag = new ClientTag(myTags[iItemStart].OPC_Tagname, curTag);
                //Client_Tags.Add(newTag);
                definitions[iCount] = new OpcDaItemDefinition();
                definitions[iCount].ItemId = myTags[iIndex].OPC_Tagname;
                definitions[iCount].IsActive = true;
                iCount++;
            }

            OpcDaItemResult[] results = currentGroup.AddItems(definitions);

            // Handle adding results.
            bItemAdded = true;
            iCount = 0;
            foreach (OpcDaItemResult result in results)
            {
                if (result.Error.Failed)
                {
                    string szMessage = "Error adding item {" + definitions[iCount].ItemId + "} Error Message {" + result.Error.ToString() + "}";
                    Log.Error(szMessage);
                    try
                    {
                        // TODO: Add this item to a bad items list to be retried
                    }
                    catch (Exception)
                    {

                    }
                }
                else
                {
                    ScanningTag isFound = Client_Tags.Find(x => x.OPC_Tagname.Contains(result.Item.ItemId));
                    if(isFound != null)
                    {
                        isFound.SetAdded(true);
                    }
                    //Log.Info("Item [" + ItemID + "] Added successfully");
                }
                iCount++;
            }

            return bItemAdded;
        }

        // This is the Async handler for the Fault Tags data updates.
        public void OnGroupValuesChanged(object sender, OpcDaItemValuesChangedEventArgs args)
        {
            // Output values.
            OpcDaGroup myGroup = (OpcDaGroup)sender;

            DateTime currentTime;
            currentTime = System.DateTime.Now;
            //FaultTag myFault = null;
            //ScanningTag myTag = null;

            foreach (OpcDaItemValue value in args.Values)
            {
                double tempValue;
                try
                {
                    tempValue = Convert.ToDouble(value.Value);
                }
                catch (Exception)
                {
                    tempValue = -999;
                }


                //Log.Info("ItemUpdate {" + value.Item.ItemId + "} Value {" + tempValue.ToString() + "} Quality {" + value.Quality + "} Timestamp {" + GetTimeString(value.Timestamp) + "}");
                Log.Info("DataUpdate Tagname,Value,QualityText,QualityValue,Timestamp," + value.Item.ItemId + "," + tempValue.ToString() + "," + value.Quality + "," + value.Quality.GetHashCode().ToString() + "," + GetTimeString(value.Timestamp));

                //myTag = Client_Tags.Find(x => x.OPC_Tagname.Contains(value.Item.ItemId));
                //if ((value.Quality.Status & OpcDaQualityStatus.Good) == OpcDaQualityStatus.Good)
                //{
                //    if (myTag != null)
                //    {
                //        //if (myTag.IsChanged(tempValue))
                //        {
                //            Log.Info("ItemUpdate {" + value.Item + "} Quality {" + value.Quality + "} Description {" + myFault.Description + "}");
                //            //SendFaultSQL(myFault, currentTime);

                //        }


                //    }

                //}
                //else
                //{

                //    string szMessage = "OPC Item [" + value.Item.ItemId + "] has come back with BAD quality. This does NOT count as a valid update";
                //    Log.Info(szMessage);

                //}
            }
        }

        //internal bool ReadValues()
        //{
        //    bool bReadSuccessful = false;
        //    bool bConnected = false;
        //    DateTime currentTime;
        //    currentTime = System.DateTime.Now;

        //    // Read all items of the group synchronously.
        //    try
        //    {
        //        bConnected = myServer.IsConnected;
        //    }
        //    catch (Exception)
        //    {
        //        bConnected = false;
        //    }
        //    if (bConnected == true)
        //    {
        //        foreach (OpcDaGroup myGroup in myServer.Groups)
        //        {
        //            if (myGroup.Name.Contains("AggregatorGroup_"))
        //            {
        //                // for each group spin a new thread and do a group read.
        //                DoBackgroundRead(myGroup);
        //            }
                    
        //        }

                
        //    }
        //    return bReadSuccessful;
        //}

        //private void DoBackgroundRead(OpcDaGroup myGroup)
        //{

        //    BackgroundWorker bw = new BackgroundWorker();

        //    // this allows our worker to report progress during work
        //    bw.WorkerReportsProgress = true;

        //    // what to do in the background thread
        //    bw.DoWork += new DoWorkEventHandler(
        //    delegate (object o, DoWorkEventArgs args)
        //    {
        //        BackgroundWorker b = o as BackgroundWorker;
        //        try
        //        {
        //            // create a new thread and process the read
        //            //ProcessRead(myGroup);
        //        }
        //        catch (Exception)
        //        {
        //        }
        //    });

        //    bw.RunWorkerAsync();
        //}

        //private void ProcessRead(OpcDaGroup myGroup)
        //{
        //    DateTime currentTime;
        //    currentTime = System.DateTime.Now;

        //    OpcDaItemValue[] values;

        //    try
        //    {
        //        //Log.Info("Sending Sync Read");
        //        string szMessage = "Sending Sync Read for " + myGroup.Name;
        //        Log.Info(szMessage);
        //        values = myGroup.Read(myGroup.Items, OpcDaDataSource.Cache);
        //        Log.Info("Read completed");

        //    }
        //    catch (Exception)
        //    {
        //        Log.Error("Exception while reading values");

        //        return;
        //    }
        //    ClientTag myTag = null;
            
            
        //    // Find the Turbine in our list that corresponds to this group.
        //    Turbine curTurbine;
        //    string szTurbineName;
        //    int iFirst, iSecond;
        //    iFirst = myGroup.Name.IndexOf('_') + 1;
        //    iSecond = myGroup.Name.Length - myGroup.Name.IndexOf('_') - 1;
        //    szTurbineName = myGroup.Name.Substring(iFirst, iSecond);
        //    curTurbine = turbine_list.Find(x => x.Name.Contains(szTurbineName));
        //    bool bGoodRead = true;

        //    foreach (OpcDaItemValue value in values)
        //    {
        //        object tempValue;
        //        try
        //        {
        //            tempValue = Convert.ToString(value.Value);
        //        }
        //        catch (Exception)
        //        {
        //            tempValue = -999;
        //        }
        //        //Console.WriteLine("Evaluating OPC Item [" + value.Item.ItemId + "] read successfully. Value: [" + tempValue + "] Quality: [" + Convert.ToDouble(value.Quality.Status) + "] Timestamp set to [" + value.Timestamp.ToString() + "]");

        //        myTag = Client_Tags.Find(x => x.OPCTag.Contains(value.Item.ItemId));
        //        if ((value.Quality.Status & OpcDaQualityStatus.Good) == OpcDaQualityStatus.Good)// || lastUpdated == DateTime.MinValue)
        //        {
        //            if (myTag != null)
        //            {
        //                //Console.WriteLine("Item found and adding to collected Values");
        //                myTag.CollectedValues.Add(value);
        //                myTag.successCount++;
        //            }
        //            //Log.Info("OPC Item [" + ItemID + "] read successfully. Value: [" + tempValue + "] Quality: [" + value.Quality.Status.ToString() + "] Timestamp set to [" + DateTime.Now + "]");

        //        }
        //        else
        //        {
        //            // Critical items are checked if they are bad quality. Any critical item that is bad in a read packet will mark the entire read as bad. (bGoodRead=false)
        //            string szMessage;
        //            if (myTag != null)
        //            {
        //                if (myTag.bCritical)
        //                {
        //                    szMessage = "Critical OPC Item [" + value.Item.ItemId + "] has come back with BAD quality. Setting Read of turbine to BAD";
        //                    Log.Warn(szMessage);
        //                    bGoodRead = false;
        //                }
        //                myTag.failCount++;
        //            }
        //            szMessage = "OPC Item [" + value.Item.ItemId + "] has come back with BAD quality. This does NOT count as a valid update";
        //            Log.Info(szMessage);
        //        }
        //    }
        //    if (curTurbine != null)
        //    {
        //        curTurbine.AddGoodRead(bGoodRead);
        //    }
            
        //}
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
            myServer = null;
        }



        internal void SetLogging(log4net.ILog log)
        {
            Log = log;
        }

        //internal void AddFaultCode(FaultCode tag)
        //{
        //    ls_FaultCodes.Add(tag);
        //}

        internal void Scan()
        {

            //ScanningTimer.Interval = 1000;// GetScanningInterval();
            //ScanningTimer.Start();

            //SQLTimer.Interval = GetSQLScanningInterval();
            //SQLTimer.Start();

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
                // Add items that were not added succesfully?

                Thread.Sleep(1000);
            }
            // Disconnect from OPC and SQL
            // Send Stop message to Fault Code table.

        }

        public void AddOPCItems()
        {
            int iGroupMax = 0;
            int iGroupCount = 0;
            int iGroupEnd = 0;
            string szGroupName;
            OpcDaGroup newGroup;
            TimeSpan UpdateRate = TimeSpan.FromMilliseconds(ScanningRate);
            OpcDaGroupState GroupState = new OpcDaGroupState(UpdateRate, false, new TimeSpan(0, 0, 0, 0), 0, null, null, null);

            iGroupMax = Client_Tags.Count / 100;
            iGroupMax = iGroupMax + 1;

            for(int i = 0; i < iGroupMax; i++)
            {
                szGroupName = "ScannerGroup_" + i;
                newGroup = myServer.AddGroup(szGroupName, GroupState);

                if (newGroup != null)
                {
                    // setup the async message handler
                    newGroup.ValuesChanged += OnGroupValuesChanged;
                    // setup the update rate to be 1 second
                    //newGroup.UpdateRate = TimeSpan.FromMilliseconds(ScanningRate);
                    // groups start inactive and then are set active after all items have been loaded. This is to improve efficiency
                    newGroup.IsActive = false;

                    newGroup.UpdateRate = UpdateRate;
                    if (Client_Tags.Count - (i * 100) < 100)
                    {
                        iGroupEnd = Client_Tags.Count;
                    }
                    else
                        iGroupEnd = (i * 100) + 100;

                    AddItems(newGroup, i*100, iGroupEnd, Client_Tags );
                    newGroup.IsActive = true;
                }
                iGroupCount++;
            }
            // add all of the fault tags into their own groups with 1 second update.
            //if(fault_tags.Count > 0)
            //{
            //    foreach (Turbine curTurbine in turbine_list)
            //    {
            //        try
            //        {
            //            newGroup = myServer.AddGroup("FaultTags_" + curTurbine.Name, GroupState);
            //            // setup the async message handler
            //            newGroup.ValuesChanged += OnGroupValuesChanged;
            //            // setup the update rate to be 1 second
            //            newGroup.UpdateRate = TimeSpan.FromMilliseconds(1000);
            //            // groups start inactive and then are set active after all items have been loaded. This is to improve efficiency
            //            newGroup.IsActive = false;
            //            AddFaultItems(newGroup, curTurbine.Name);
                        
            //        }
            //        catch(Exception)
            //        {

            //        }
            //    }
            //}
        }

        //private void AddFaultItems(OpcDaGroup newGroup, string curTurbine)
        //{

        //    OpcDaItemDefinition[] definitions = new OpcDaItemDefinition[fault_tags.Count / turbine_list.Count];
        //    int iCount = 0;
        //    foreach (FaultTag curTag in fault_tags)
        //    {
        //        if (curTurbine == curTag.GetTurbine())
        //        {
        //            definitions[iCount] = new OpcDaItemDefinition();
        //            definitions[iCount].ItemId = curTag.OPCTag;
        //            definitions[iCount].IsActive = true;
        //            iCount++;
        //        }
        //    }

        //    OpcDaItemResult[] results = newGroup.AddItems(definitions);

        //    // Handle adding results.
        //    iCount = 0;
        //    foreach (OpcDaItemResult result in results)
        //    {
        //        if (result.Error.Failed)
        //        {
        //            string szMessage = "Error adding item {" + definitions[iCount].ItemId + "} Error Message {" + result.Error.ToString() + "}";
        //            Log.Warn(szMessage);

        //            try
        //            {
        //                // TODO: Add this item to a bad items list to be retried
        //            }
        //            catch (Exception)
        //            {

        //            }
        //        }
        //        else
        //        {
        //            FaultTag isFound = fault_tags.Find(x => x.OPCTag.Contains(result.Item.ItemId));
        //            if (isFound != null)
        //            {
        //                isFound.SetAdded(true);
        //            }
        //            //Log.Info("Item [" + ItemID + "] Added successfully");
        //        }
        //        iCount++;
        //    }
            
        //}

        private void OnTimedScanTime(Object source, ElapsedEventArgs e)
        {
            //ReadValues();

            //// Re-adjust the timer to ensure we run on at the 10 second mark
            //string szMessage = "The Elapsed Scanned event was raised at {" + e.SignalTime.ToLongTimeString() + "}";
            //Log.Info(szMessage);
            //if (bScanning)
            //{
            //    ScanningTimer.Interval = GetScanningInterval();
            //    //szMessage = "New Interval is {" + ScanningTimer.Interval.ToString() + "}";
            //    //Log.Info(szMessage);
            //    ScanningTimer.Start();
            //}
           
        }

        // Get how long it is to execute closest to the next scan interval.
        private double GetScanningInterval()
        {
            DateTime now = DateTime.Now;
            double iMilliseconds = (((ScanningRate / 1000) - (now.Second % 10)) * 1000 - now.Millisecond);

            return iMilliseconds;
        }


        // Get how long it is to execute closest to the next SQL Push time.
        //private double GetSQLScanningInterval()
        //{
        //    DateTime now = DateTime.Now;
        //    // total scan time  - (Minutes % 10 - seconds - milliseconds)
        //    //double iMilliseconds = (((SQLUpdateRate / 1000) - (now.Second % 10)) * 1000 - now.Millisecond);
        //    // 300 000 - 3 
        //    double iMilliseconds = SQLUpdateRate - (((now.Minute % (SQLUpdateRate / 60000)) * 60000) + (now.Second * 1000) + now.Millisecond);

        //    //Console.WriteLine("SQL Scanning Interval {0}",
        //    //      iMilliseconds);
        //    //Console.WriteLine("SQLUpdateRate {0} - (Now.Minute {1} % {2} + ((now.Second % 10) * 1000) {3} + Millisecond {4} : CurrentTime {5:HH:mm:ss.fff}",
        //    //        SQLUpdateRate, now.Minute, SQLUpdateRate / 60000, ((now.Second % 10) * 1000), now.Millisecond, now);
        //    return iMilliseconds;
        //}

        //private void OnTimedSQLPush(Object source, ElapsedEventArgs e)
        //{
        //    string szMessage = "The Elapsed SQL Event was raised at {" + e.SignalTime.ToLongTimeString() + "}";
        //    Log.Info(szMessage);
        //    SQLConnect();

        //    if (bScanning)
        //    {
        //        SQLTimer.Interval = GetSQLScanningInterval();
        //        SQLTimer.Start();
        //    }
        //}

        //private bool SendFaultSQL(FaultTag myTag, DateTime currentTime)
        //{

        //    string szNewConnection;
        //    szNewConnection = "Data Source=" + SQLServer + ";Initial Catalog=" + SQLCatalog + ";User ID=" + SQLUser + ";Password=" + SQLPass + ";";

        //    SqlConnection SQLConn = new SqlConnection(szNewConnection);
        //    CalcColumn ResultHolder;

        //    try
        //    {
        //        SQLConn.Open();
        //        List<CalcColumn> ResultList = new List<CalcColumn>();
        //        string szTime = GetTimeString(currentTime);
                
        //        ResultHolder = new CalcColumn(szTime, "Timestamp");
        //        ResultList.Add(ResultHolder);
        //        ResultHolder = new CalcColumn(myTag.GetTurbine(), SQLNameColumn);
        //        ResultList.Add(ResultHolder);
        //        ResultHolder = new CalcColumn(myTag.UserCode, "UserCode");
        //        ResultList.Add(ResultHolder);
        //        ResultHolder = new CalcColumn(myTag.GetLastFault().ToString(), "Value");
        //        ResultList.Add(ResultHolder);
        //        ResultHolder = new CalcColumn(myTag.Description, "Description");
        //        ResultList.Add(ResultHolder);

        //        string SQLCommandString = "Insert into dbo.[" + SQLFaultTable + "] (";
        //        int iCount = 1;
        //        foreach (CalcColumn curCalc in ResultList)
        //        {
        //            SQLCommandString += curCalc.ColumnName;
        //            if (iCount != ResultList.Count())
        //            {
        //                SQLCommandString += ",";
        //            }

        //            iCount++;
        //        }
        //        iCount = 1;
        //        SQLCommandString += ") VALUES (";
        //        foreach (CalcColumn curCalc in ResultList)
        //        {
        //            SQLCommandString += "@" + curCalc.ColumnName;
        //            if (iCount != ResultList.Count())
        //            {
        //                SQLCommandString += ",";
        //            }

        //            iCount++;
        //        }
        //        SQLCommandString += ")";

        //        SqlCommand command = new SqlCommand(SQLCommandString, SQLConn);

        //        foreach (CalcColumn curCalc in ResultList)
        //        {
        //            command.Parameters.AddWithValue("@" + curCalc.ColumnName, curCalc.CalcValue);
        //        }

        //        Log.Info(SQLCommandString);
        //        int result = command.ExecuteNonQuery();


        //        // Check Error
        //        if (result < 0)
        //            //Console.WriteLine("Error inserting data into Database!");
        //            Log.Error("Error inserting data into the Database!");
        //    }
        //    catch (System.Data.SqlClient.SqlException e)
        //    {
        //        string szMessage = "Error while inserting data to the SQL Database : " + e.Message;
        //        Log.Error(szMessage);
        //    }
        //    finally
        //    {
        //        if (SQLConn != null)
        //            SQLConn.Close();
        //    }
                
        //    return true;
        //}

        //private bool SQLConnect()
        //{

        //    try
        //    {
        //        DateTime currentTime;
        //        currentTime = System.DateTime.Now;
        //        if (SQLTable != "")
        //        {
        //            foreach (Turbine curTurbine in turbine_list)
        //            {
        //                BuildSQLCommand(curTurbine, currentTime);
        //            }
        //        }
        //        else
        //        {
        //            Log.Warn("SQL Table configuration is blank. Cannot send Insert Statement.");
        //        }
        //    }
        //    finally
        //    {
        //        // Clear all of the collected values to start collecting a new data set
        //        foreach(ClientTag myTag in Client_Tags)
        //        {
        //            myTag.CollectedValues.Clear();
        //        }
        //    }
        //    return true;
        //}
        public string GetTimeString(DateTimeOffset curTime)
        {
            string szTime;
            string szMinute;
            string szSecond;

            if (curTime.Minute < 10)
                szMinute = "0" + curTime.Minute.ToString();
            else
                szMinute = curTime.Minute.ToString();

            if (curTime.Second < 10)
                szSecond = "0" + curTime.Second.ToString();
            else
                szSecond = curTime.Second.ToString();

            szTime = curTime.Date.ToShortDateString() + " " + curTime.Hour.ToString() + ":" + szMinute + ":" + szSecond + "." + curTime.Millisecond.ToString();

            return szTime;
        }
    //    private void BuildSQLCommand(Turbine curTurbine, DateTime currentTime) //SqlConnection SQLConn, string szTurbine)
    //    {

    //        string szNewConnection;
    //        szNewConnection = "Data Source=" + SQLServer + ";Initial Catalog=" + SQLCatalog + ";User ID=" + SQLUser + ";Password=" + SQLPass + ";";

    //        SqlConnection SQLConn = new SqlConnection(szNewConnection);
    //        string szTransform;
    //        string szColumn;
    //        string szResult;

    //        string[] transformarray;
    //        string[] columnsArray;
    //        CalcColumn ResultHolder;

    //        if (curTurbine.GoodReads + curTurbine.BadReads > 1)
    //        {
    //            try
    //            {
    //                Log.Info("SQL Connection String: {" + szNewConnection + "}");
    //                SQLConn.Open();
    //                //Log.Info("SQL Connection Opened");
    //                List<CalcColumn> ResultList = new List<CalcColumn>();
    //                // if we managed to poll a few milliseconds to early then round up the seconds to 10.
    //                if (currentTime.Second == 59)
    //                {
    //                    currentTime = currentTime.AddSeconds(1);
    //                }

    //                ResultHolder = new CalcColumn(currentTime.ToString(), "TIMESTAMP");
    //                ResultList.Add(ResultHolder);
    //                ResultHolder = new CalcColumn(curTurbine.Name, SQLNameColumn);
    //                ResultList.Add(ResultHolder);

    //                // if There was an extra sample or missing one sample then round the sample down to the nearest 60 for accounting purposes.
    //                // This only works for 10 second scan rate and 10 minute push rate
    //                if (curTurbine.GoodReads == 59 || curTurbine.GoodReads == 61)
    //                {
    //                    curTurbine.GoodReads = 60;
    //                }
    //                ResultHolder = new CalcColumn((curTurbine.GoodReads * ScanningRate / 1000).ToString(), "GOOD_SECONDS");
    //                ResultList.Add(ResultHolder);
    //                // if There was an extra sample or missing one sample then round the sample down to the nearest 60 for accounting purposes.
    //                // This only works for 10 second scan rate and 10 minute push rate
    //                if (curTurbine.BadReads == 59 || curTurbine.BadReads == 61)
    //                {
    //                    curTurbine.BadReads = 60;
    //                }
    //                ResultHolder = new CalcColumn((curTurbine.BadReads * ScanningRate / 1000).ToString(), "BAD_SECONDS");
    //                ResultList.Add(ResultHolder);
    //                ResultHolder = new CalcColumn(((curTurbine.GoodReads + curTurbine.BadReads) * ScanningRate / 1000).ToString(), "TOTAL_SECONDS");
    //                ResultList.Add(ResultHolder);
    //                curTurbine.ClearReads();
    //                ClientTag tempTag = Client_Tags.First();

    //                foreach (ClientTag curTag in Client_Tags)
    //                {
    //                    if (curTag.OPCTag.Contains(curTurbine.Name))
    //                    {
    //                        transformarray = curTag.Transforms.ToArray();
    //                        columnsArray = curTag.WBI_Columns.ToArray();
    //                        for (int i = 0; i < transformarray.Count(); i++)
    //                        {
    //                            szTransform = transformarray[i];
    //                            szColumn = columnsArray[i];
    //                            //Log.Info("Do Calculation for {" + curTag.OPCTag + "}");
    //                            szResult = DoCalculations(szTransform, curTag.CollectedValues);
    //                            //Log.Info("Calculation returned {" + szResult + "}");
    //                            if (szResult != "")
    //                            {
    //                                ResultHolder = new CalcColumn(szResult, szColumn);
    //                                ResultList.Add(ResultHolder);
    //                            }

    //                        }
    //                    }
    //                }
    //                //Log.Info("Finished Client Tag iteration");

    //                string SQLCommandString = "Insert into dbo.[" + SQLTable + "] (";
    //                int iCount = 1;
    //                foreach (CalcColumn curCalc in ResultList)
    //                {
    //                    SQLCommandString += curCalc.ColumnName;
    //                    if (iCount != ResultList.Count())
    //                    {
    //                        SQLCommandString += ",";
    //                    }

    //                    iCount++;
    //                }
    //                iCount = 1;
    //                SQLCommandString += ") VALUES (";
    //                foreach (CalcColumn curCalc in ResultList)
    //                {
    //                    SQLCommandString += "@" + curCalc.ColumnName;
    //                    if (iCount != ResultList.Count())
    //                    {
    //                        SQLCommandString += ",";
    //                    }

    //                    iCount++;
    //                }
    //                SQLCommandString += ")";

    //                SqlCommand command = new SqlCommand(SQLCommandString, SQLConn);

    //                foreach (CalcColumn curCalc in ResultList)
    //                {
    //                    command.Parameters.AddWithValue("@" + curCalc.ColumnName, curCalc.CalcValue);
    //                }

    //                Log.Info("SQL Command String: {" + SQLCommandString + "}");
    //                int result = command.ExecuteNonQuery();
    //                //Log.Info("SQL Execute Completed");


    //                // Check Error
    //                if (result < 0)
    //                    //Console.WriteLine("Error inserting data into Database!");
    //                    Log.Error("Error inserting data into the Database!");
    //            }
    //            catch (System.Data.SqlClient.SqlException e)
    //            {
    //                string szMessage = "Error while inserting data to the SQL Database : " + e.Message;
    //                Log.Error(szMessage);
    //            }
    //            finally
    //            {
    //                if (SQLConn != null)
    //                    SQLConn.Close();
    //            }
    //        }
    //    }

    //    private string DoCalculations(string szTransform, List<OpcDaItemValue> valueCollection)
    //    {
    //        string szResult = "";
    //        double dValue = -99887766; // initial value to be replaced
    //        List<double> valList;
    //        int secSeq = -1;
    //        try
    //        {
    //            if (valueCollection.Count > 0)
    //            {
    //                valList = new List<double>();
    //                foreach (OpcDaItemValue val in valueCollection)
    //                {
    //                    valList.Add(Convert.ToDouble(val.Value));
    //                }

    //                switch (szTransform.ToUpper())
    //                {
    //                    case "MAX":
    //                        dValue = Convert.ToDouble(valList.Max());
    //                        break;
    //                    case "MIN":
    //                        dValue = Convert.ToDouble(valList.Min());
    //                        break;
    //                    case "MEAN":
    //                        if (valList.Count > 0)
    //                            dValue = valList.Average();
    //                        else
    //                            dValue = 0;
    //                        break;
    //                    case "STDDEV":
    //                        if (valList.Count > 0)
    //                            dValue = CalculateStdDev(valList);
    //                        else
    //                            dValue = 0;
    //                        break;
    //                    case "ONLINESECS":
    //                        int iGoodCount = 0;
    //                        foreach (OpcDaItemValue val in valueCollection)
    //                        {
    //                            if (Convert.ToDouble(val.Value) != 0)
    //                            {
    //                                iGoodCount++;
    //                            }
    //                        }
    //                        dValue = iGoodCount * (ScanningRate / 1000);
    //                        break;
    //                    case "OFFLINESECS":
    //                        int iBadCount = 0;
    //                        foreach (OpcDaItemValue val in valueCollection)
    //                        {
    //                            if (Convert.ToDouble(val.Value) == 0)
    //                            {
    //                                iBadCount++;
    //                            }
    //                        }
    //                        dValue = iBadCount * (ScanningRate / 1000);
    //                        break;
    //                    case "ENERGYNET":
    //                        if (valList.Count > 0)
    //                        {
    //                            dValue = valList.Sum();
    //                            dValue = dValue / 360;
    //                        }
    //                        else
    //                            dValue = 0;
    //                        break;
    //                    case "SECSEQ0":
    //                        secSeq = 0;
    //                        dValue = GetSeqCalculation(secSeq, valueCollection);
    //                        break;
    //                    case "SECSEQ25":
    //                        secSeq = 25;
    //                        dValue = GetSeqCalculation(secSeq, valueCollection);

    //                        break;
    //                    case "SECSEQ50":
    //                        secSeq = 50;
    //                        dValue = GetSeqCalculation(secSeq, valueCollection);

    //                        break;
    //                    case "SECSEQ75":
    //                        secSeq = 75;
    //                        dValue = GetSeqCalculation(secSeq, valueCollection);

    //                        break;
    //                    case "SECSEQ100":
    //                        secSeq = 100;
    //                        dValue = GetSeqCalculation(secSeq, valueCollection);

    //                        break;
    //                    case "SECSEC125":
    //                        secSeq = 125;
    //                        dValue = GetSeqCalculation(secSeq, valueCollection);

    //                        break;

    //                    // any Translation that is not recognized will be calculated as LAST
    //                    case "LAST":
    //                    default:

    //                        dValue = Convert.ToDouble(valueCollection.Last().Value);

    //                        break;

    //                }

    //                valList.Clear();
    //                if (dValue == -99887766)
    //                    szResult = "";
    //                else
    //                    szResult = dValue.ToString();
    //            }
    //        }
    //        catch(Exception e)
    //        {
    //            Log.Error("Exception thrown in DoCalculations: Transform: " + szTransform + " Exception: " + e.Message);
    //            szResult = "";
    //        }
    //        return szResult;
    //    }

    //    private double GetSeqCalculation(int secSeq, List<OpcDaItemValue> valueCollection)
    //    {
    //        double dValue = -99887766;
    //        int iCount = 0;
    //        foreach (OpcDaItemValue val in valueCollection)
    //        {
    //            if (Convert.ToDouble(val.Value) > (secSeq - 0.25) && Convert.ToDouble(val.Value) < (secSeq + 0.25))
    //            {
    //                iCount++;
    //            }
    //        }
    //        dValue = iCount * (ScanningRate / 1000);
    //        return dValue;
    //    }

    //    private double CalculateStdDev(IEnumerable<double> values)
    //    {
    //        double ret = 0;
    //        if (values.Count() > 0)
    //        {
    //            //Compute the Average      
    //            double avg = values.Average();
    //            //Perform the Sum of (value-avg)_2_2      
    //            double sum = values.Sum(d => Math.Pow(d - avg, 2));
    //            //Put it all together      
    //            ret = Math.Sqrt((sum) / (values.Count() - 1));
    //        }
    //        return ret;
    //    }
    
    }
}
