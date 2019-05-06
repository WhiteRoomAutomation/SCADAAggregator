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

namespace SCADAAggregator
{
    public class ClientTag
    {
        public ClientTag(string newItem, AggregatorTag newTag)
        {
            CollectedValues = new List<OpcDaItemValue>();
            OPCTag = newItem;
            Scaling = newTag.Scaling;
            WBI_Column = newTag.WBI_Column;
            Transform = newTag.Transform;
            bAdded = false;
        }
        public List<OpcDaItemValue> CollectedValues;
        protected OpcDaItem myItem;
        public string OPCTag { get; set; }
        public string Scaling { get; set; }
        public string WBI_Column { get; set; }
        public string Transform { get; set; }
        public bool bAdded { get; set; }
        
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
        //public List<AggregatorTag> opc_met_tags;
        public List<FaultCode> ls_FaultCodes;
        public List<string> turbine_list;
        //public List<string> met_list;
        OpcDaServer myServer;
        public List<OpcDaGroup> DAGroups;
        public bool bScanning;
        protected System.Timers.Timer ScanningTimer;
        protected System.Timers.Timer SQLTimer;

        public OPCClient(string newProgID)
        {
            opc_tags = new List<AggregatorTag>();
            //opc_met_tags = new List<AggregatorTag>();
            ls_FaultCodes = new List<FaultCode>();
            turbine_list = new List<string>();
            DAGroups = new List<OpcDaGroup>();
            //met_list = new List<string>();
            this.ProgID = newProgID;
            bScanning = true;
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
            }
            else
            {
                newTag.Transforms.Add(newTag.Transform);
                opc_tags.Add(newTag);
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
            foreach (AggregatorTag curTag in opc_tags)
            {
                iIndex = curTag.OPCTag.IndexOf('%');
                trueTagname = curTag.OPCTag.Substring(0, iIndex) + curTurbine + curTag.OPCTag.Substring(iIndex+1);
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
                    bItemAdded = false;
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
                    //Log.Info("Item [" + ItemID + "] Added successfully");
                }
            }

            return bItemAdded;
        }

        //internal bool AddItem()
        //{
        //    bool bItemAdded = false;
        //    bool bConnected = false;
        //    try
        //    {
        //        bConnected = myServer.IsConnected;
        //    }
        //    catch (Exception)
        //    {
        //        bConnected = false;
        //    }
        //    if (bConnected)
        //    {
        //        // Create a group with items.
        //        try
        //        {
        //            myGroup = myServer.AddGroup("AutoRestartGroup");

        //        }
        //        catch (Exception)
        //        {
        //            return false;
        //        }

        //        // Configure subscription.
        //        myGroup.ValuesChanged += OnGroupValuesChanged;
        //        myGroup.UpdateRate = TimeSpan.FromMilliseconds(1000); // ValuesChanged won't be triggered if zero
        //        myGroup.IsActive = true;

        //        var definition = new OpcDaItemDefinition
        //        {
        //            ItemId = ItemID,
        //            IsActive = true
        //        };
        //        OpcDaItemDefinition[] definitions = { definition };
        //        OpcDaItemResult[] results = myGroup.AddItems(definitions);

        //        // Handle adding results.
        //        bItemAdded = true;
        //        foreach (OpcDaItemResult result in results)
        //        {
        //            if (result.Error.Failed)
        //            {
        //                //Console.WriteLine("Error adding items: {0}", result.Error);
        //                //Log.Error("Error adding item: [" + ItemID + "] Error: [" + result.Error.ToString() + "]");
        //                bItemAdded = false;
        //                try
        //                {
        //                    Disconnect();
        //                }
        //                catch (Exception)
        //                {

        //                }
        //            }
        //            else
        //            {
        //                //Log.Info("Item [" + ItemID + "] Added successfully");
        //            }
        //        }
        //    }
        //    return bItemAdded;
        //}

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
            Log.Info("Reading Values");

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
                foreach (OpcDaGroup myGroup in DAGroups)
                {
                    try
                    {
                        //Log.Info("Sending Sync Read");
                        values = myGroup.Read(myGroup.Items);//, OpcDaDataSource.Device);

                    }
                    catch (Exception)
                    {
                        Log.Info("Exception while reading values");

                        return false;
                    }
                    foreach (OpcDaItemValue value in values)
                    {
                        //object tempValue;
                        ////value.Value;
                        //try
                        //{
                        //    tempValue = Convert.ToString(value.Value);
                        //}
                        //catch (Exception)
                        //{
                        //    tempValue = -999;
                        //}
                        //Log.Info("Evaluating OPC Item [" + ItemID + "] read successfully. Value: [" + tempValue + "] Quality: [" + Convert.ToDouble(value.Quality.Status) + "] Timestamp set to [" + DateTime.Now + "]");
                        //if ((value.Quality.Status & OpcDaQualityStatus.Good) == OpcDaQualityStatus.Good || lastUpdated == DateTime.MinValue)
                        //{
                        //    //Log.Info("OPC Item [" + ItemID + "] read successfully. Value: [" + tempValue + "] Quality: [" + value.Quality.Status.ToString() + "] Timestamp set to [" + DateTime.Now + "]");
                        //    if (tempValue != Value)
                        //    {
                        //        Value = tempValue;
                        //        lastUpdated = DateTime.Now;
                        //        Log.Info("OPC Item [" + ItemID + "] has changed. Updating stored value and timestamp");
                        //    }
                        //    else
                        //    {
                        //        Log.Info("OPC Item [" + ItemID + "] has NOT changed. Ignoring read");
                        //    }
                        //}
                        //else
                        //{
                        //    Log.Warn("OPC Item [" + ItemID + "] has come back with BAD quality. This does NOT count as a valid update");
                        //}
                        //bReadSuccessful = true;
                        ////Console.WriteLine("Item: " + value.Item.ItemId.ToString() + " Value: " + value.Value.ToString());
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
                if( ScanningTimer.Enabled == false)
                {
                    DateTime nowTime = DateTime.Now;
                    Debug.Print("Seconds is {0}", nowTime.Second);
                    if (nowTime.Second % 10 == 0)
                    {
                        //ScanningTimer.Enabled = true;
                        ScanningTimer.Start();
                        Debug.Print("Enabling Scan Scanning Timer");
                        
                    }
                }

                if (SQLTimer.Enabled == false)
                {
                    DateTime nowTime = DateTime.Now;
                    //Debug.Print("Minutes is {0}", nowTime.Minute);
                    if (nowTime.Minute % 10 == 0)
                    {
                        SQLTimer.Start();
                        Debug.Print("Enabling SQL Scanning Timer");

                    }
                }
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
            TimeSpan UpdateRate = new TimeSpan(0,0,0,10);
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
            //this.ReadValues();

            // Re-adjust the timer to ensure we run on at the 10 second mark
            Console.WriteLine("The Elapsed Scanned event was raised at {0:HH:mm:ss.fff}",
                  e.SignalTime);
            ScanningTimer.Interval = GetScanningInterval();
            ScanningTimer.Start();
           
        }

        private double GetScanningInterval()
        {
            DateTime now = DateTime.Now;
            double iMilliseconds = ((10 - (now.Second % 10)) * 1000 - now.Millisecond);

            Console.WriteLine("Scanning Interval {0}",
                  iMilliseconds);
            return iMilliseconds;
        }

        private void OnTimedSQLPush(Object source, ElapsedEventArgs e)
        {
            Console.WriteLine("The Elapsed SQL event was raised at {0:HH:mm:ss.fff}",
                              e.SignalTime);
        }

        private bool SQLConnect()
        {
            throw new NotImplementedException();
        }

        private bool IsSQLConnected()
        {
            throw new NotImplementedException();
        }
    
}
}
