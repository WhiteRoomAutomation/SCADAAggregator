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
using System.Runtime.InteropServices;
using log4net;

//using System.Runtime.InteropServices;

namespace SCADAAggregator
{
    public class AggregatorTag
    {

        public AggregatorTag()
        {
            Transforms = new List<string>();
            WBI_Columns = new List<string>();
            CollectedValues = new List<OpcDaItemValue>();
            bAdded = false;
        }

        public string OPCServer { get; set; }
        public string OPCTag { get; set; }
        //public string Scaling { get; set; }
        //public string SQL_Format { get; set; }
        //public string ScanRate { get; set; }
        //public string ServerName { get; set; }
        public string WBI_Column { get; set; }
        public string Transform { get; set; }
        public List<string> Transforms;
        public List<string> WBI_Columns { get; set; }
        public List<OpcDaItemValue> CollectedValues;
        private bool bAdded { get; set; }
        private string FullTagname { get; set; }
        public void SetTagname(string newTagname)
        {
            FullTagname = newTagname;
        }
        public bool GetTagname(string newTagname)
        {
            bool bRetVal = false;
            if (FullTagname.Contains(newTagname))
                bRetVal = true;
            return bRetVal;
        }


    }

    public class FaultCode
    {
        public string Number { get; set; }
        public string Description { get; set; }
        public string OpStat { get; set; }
    }

    public enum ServiceState
    {
        SERVICE_STOPPED = 0x00000001,
        SERVICE_START_PENDING = 0x00000002,
        SERVICE_STOP_PENDING = 0x00000003,
        SERVICE_RUNNING = 0x00000004,
        SERVICE_CONTINUE_PENDING = 0x00000005,
        SERVICE_PAUSE_PENDING = 0x00000006,
        SERVICE_PAUSED = 0x00000007,
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct ServiceStatus
    {
        public int dwServiceType;
        public ServiceState dwCurrentState;
        public int dwControlsAccepted;
        public int dwWin32ExitCode;
        public int dwServiceSpecificExitCode;
        public int dwCheckPoint;
        public int dwWaitHint;
    };

    public partial class SCADA_Aggregator : ServiceBase
    {

        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern bool SetServiceStatus(System.IntPtr handle, ref ServiceStatus serviceStatus);

        public static log4net.ILog Log { get; private set; }
        static System.Timers.Timer myTimer = new System.Timers.Timer();
        //static int alarmCounter = 1;
        //static bool exitFlag = false;
        public XmlDocument Config;
        public int EventID = 0;
        public List<OPCClient> OPCClientsList;
        private List<Thread> myThreads;
        public DateTime LastExecuteTime;

        private int ScanningTimer = 0;
        private int SQLTimer = 0;
        private string SQLServer = "";
        private string SQLUser = "";
        private string SQLPass = "";
        private string SQLTable = "";
        private string SQLDB = "";
        private string SQLCatalog = "";

        public SCADA_Aggregator()
        {
            InitializeComponent();
        }

        public void OnDebug()
        {
            OnStart(null);
        }

        protected override void OnStart(string[] args)
        {

            ServiceStatus serviceStatus = new ServiceStatus();
            serviceStatus.dwCurrentState = ServiceState.SERVICE_START_PENDING;
            serviceStatus.dwWaitHint = 100000;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);

            // load config settings
            LoadOPCConfiguration();
            CreateThreads();

            //OPCClientsList.First().Scan();
            // New thread for each OPC Server.
            // for each server connect to the OPC Server
            // New thread for each OPC Server to havea  10 minute push to SQL
            // Connect to the OPC Server
            // Connect to the SQL Server

            serviceStatus.dwCurrentState = ServiceState.SERVICE_RUNNING;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);
        }

        private void CreateThreads()
        {
            foreach(OPCClient curServer in OPCClientsList)
            {
                curServer.InitializeComponent();
                ThreadStart childref = new ThreadStart(curServer.Scan);
                Thread childThread = new Thread(childref);
                myThreads.Add(childThread);
                childThread.Start();
                
            }
            //throw new NotImplementedException();
        }

        private void LoadOPCConfiguration()
        {
            //LoadTaglistConfiguration("KMW_AGG2.csv");
            //LoadTaglistConfiguration("KMW_MET_CONFIG.csv");
            //LoadTurbineNameConfiguration("TurbNameList.txt");
            //LoadTurbineNameConfiguration("METNameList.txt");
            LoadXMLConfiguration();

            //LoadTaglistConfiguration("SimulatedTags.csv");
            //LoadTurbineNameConfiguration("SimTurbines.txt");

            //LoadMetTaglistConfiguration();
            //LoadMetNameConfiguration();
            LoadFaultListConfiguration();

            // Get Taglist CSV -- Done
            // Get Turbine Name CSV
            // Get MetTaglist CSV
            // Get MetName CSV
            // get FaultList CSV
        }

        private void LoadXMLConfiguration()
        {
            Config = new XmlDocument();
            try { Config.Load("AggregatorConfig.xml"); }
            catch (System.IO.FileNotFoundException)
            {
                Config.LoadXml("<?xml version=\"1.0\"?> \n" +
                "<Configuration ScanningTimer=\"10000\" SQLTimer=\"600000\" SQLServer=\"127.0.0.1\" SQLUser=\"sa\" SQLPass=\"sa\" SQLDB=\"SQLSERVER\" SQLCatalog=\"WCS\" SQLTable=\"turb_ten_min\"> \n" +
                "   <OPCServers> \n" +
                "       <Config Taglist=\"KMW_AGG2.csv\" Turbines=\"TurbNameList.txt\"/> \n" +
                "       <Config Taglist=\"KMW_MET_CONFIG.csv\" Turbines=\"METNameList.txt\"/> \n" +
                "   </OPCServers> \n" +
                "</Configuration>");
            }
            // SQL address and login info

            //Log.Info($"{nameof(Service.WinService)} Start command received.");

            XmlNode configNode = Config.SelectSingleNode("/Configuration");
            XmlNode tempAttribute;
            XmlNode OPCNode = configNode.FirstChild; // get the <OPCServers> node
            XmlNode nodeList = OPCNode.FirstChild;
            //int ScanningTimer = 0;
            //int SQLTimer = 0;
            //string SQLServer = "";
            //string SQLUser = "";
            //string SQLPass = "";
            tempAttribute = configNode.Attributes.GetNamedItem("ScanningTimer");
            if (tempAttribute != null)
                ScanningTimer = Convert.ToInt32(tempAttribute.Value);
            else
                ScanningTimer = 10000;
            tempAttribute = configNode.Attributes.GetNamedItem("SQLTimer");
            if (tempAttribute != null)
                SQLTimer = Convert.ToInt32(tempAttribute.Value);
            else
                SQLTimer = 600000;
            tempAttribute = configNode.Attributes.GetNamedItem("SQLServer");
            if (tempAttribute != null)
                SQLServer = tempAttribute.Value;
            else
                SQLServer = "127.0.0.1";
            tempAttribute = configNode.Attributes.GetNamedItem("SQLUser");
            if (tempAttribute != null)
                SQLUser = tempAttribute.Value;
            else
                SQLUser = "sa";
            tempAttribute = configNode.Attributes.GetNamedItem("SQLPass");
            if (tempAttribute != null)
                SQLPass = tempAttribute.Value;
            else
                SQLPass = "sa";

            tempAttribute = configNode.Attributes.GetNamedItem("SQLDB");
            if (tempAttribute != null)
                SQLDB = tempAttribute.Value;
            else
                SQLDB = "SQLEXPRESS";

            tempAttribute = configNode.Attributes.GetNamedItem("SQLCatalog");
            if (tempAttribute != null)
                SQLCatalog = tempAttribute.Value;
            else
                SQLCatalog = "WCS";

            tempAttribute = configNode.Attributes.GetNamedItem("SQLTable");
            if (tempAttribute != null)
                SQLTable = tempAttribute.Value;
            else
                SQLTable = "turb_ten_min";

            while (nodeList != null)
            {
                tempAttribute = nodeList.Attributes.GetNamedItem("Taglist");
                if (tempAttribute != null)
                    //SQLPass = tempAttribute.Value;
                    LoadTaglistConfiguration(tempAttribute.Value);

                tempAttribute = nodeList.Attributes.GetNamedItem("Turbines");
                if (tempAttribute != null)
                    //SQLPass = tempAttribute.Value;
                    LoadTurbineNameConfiguration(tempAttribute.Value);
                nodeList = nodeList.NextSibling;
            }
        }

        private void LoadFaultListConfiguration()
        {
            var reader = new StreamReader(AppDomain.CurrentDomain.BaseDirectory + "FaultList.csv");
            var csv = new CsvReader(reader);
            {
                var records = csv.GetRecords<FaultCode>();
                //string opcName;
                //opcServer CurrentServer = null;
                foreach (FaultCode tag in records)
                {
                    foreach (OPCClient curServer in OPCClientsList)
                    {
                        curServer.AddFaultCode(tag);
                    }
                }
            }
            reader.Close();
            //throw new NotImplementedException();
        }

        //private void LoadMetNameConfiguration()
        //{
        //    int counter = 0;
        //    string METName;

        //    // Read the file and display it line by line.  
        //    System.IO.StreamReader file =
        //        new System.IO.StreamReader(AppDomain.CurrentDomain.BaseDirectory + "METNameList.txt");
        //    while ((METName = file.ReadLine()) != null)
        //    {
        //        foreach (OPCClient curServer in OPCClientsList)
        //        {
        //            curServer.AddTurbine(METName);
        //        }
        //        //System.Console.WriteLine(line);
        //        counter++;
        //    }

        //    file.Close();
        //    //throw new NotImplementedException();
        //}

        private void LoadTurbineNameConfiguration(string TurbineFileName)
        {

            //var reader = new StreamReader(AppDomain.CurrentDomain.BaseDirectory + "KMW_MET_CONFIG.csv");

            int counter = 0;
            string TurbineName;

            // Read the file and display it line by line.  
            System.IO.StreamReader file =
                new System.IO.StreamReader(AppDomain.CurrentDomain.BaseDirectory + TurbineFileName);
            while ((TurbineName = file.ReadLine()) != null)
            {
                foreach(OPCClient curServer in OPCClientsList)
                {
                    curServer.AddTurbine(TurbineName);
                }
                //System.Console.WriteLine(line);
                counter++;
            }

            file.Close();

            //throw new NotImplementedException();
        }

        protected override void OnStop()
        {
            foreach(OPCClient curClient in OPCClientsList)
            {
                curClient.bScanning = false;
            }
            // disconnect OPC Server
            // Disconnect SQL Server
        }

        void PrintConfiguration()
        {
            //XmlNode mainNode = Config.SelectSingleNode("/Configuration");
            //XmlNode configNode = mainNode.FirstChild; // get the <Applications> node
            //XmlNode nodelist = configNode.FirstChild; // get the first <Process> node
            //string process = "";
            //string action = "";
            //string CPULimit = "";
            //string MemoryLimit = "";
            //string WaitTime = "";

            //string progid = "";
            //string itemid = "";
            //string actionOPC = "";
            //string staletimeout = "";
            //string WaitTimeOPC = "";
            //string ConnectTimeout = "";

            //while (nodelist != null) // for each node
            //{
            //    if (nodelist.Name == "Process")
            //    {
            //        //GetAttributesProcess(ref nodelist, ref process, ref action, ref CPULimit, ref MemoryLimit, ref WaitTime);
            //        try
            //        {
            //            Log.Info("Process Being Monitored: " + process);
            //            Log.Info("Action to be taken: " + action);
            //            Log.Info("CPU Limit: " + CPULimit);
            //            Log.Info("Memory Limit: " + MemoryLimit);
            //            Log.Info("WaitTime: " + WaitTime);
            //            Log.Info("");
            //        }
            //        catch (Exception ex)
            //        {
            //            Log.Error("Error in Print Configuration Process: " + ex.Message);
            //        }
            //    }
            //    else if (nodelist.Name == "OPC")
            //    {
            //        GetAttributesOPC(ref nodelist, ref progid, ref itemid, ref staletimeout, ref actionOPC, ref WaitTimeOPC, ref ConnectTimeout);
            //        try
            //        {
            //            Log.Info("OPC Server: " + progid);
            //            Log.Info("OPC ItemID: " + itemid);
            //            Log.Info("Action to be taken: " + actionOPC);
            //            Log.Info("Stale Timeout: " + staletimeout);
            //            Log.Info("WaitTime: " + WaitTimeOPC);
            //            Log.Info("ConnectTimeout: " + ConnectTimeout);
            //            Log.Info("");
            //            LoadOPCConfiguration(progid, itemid, staletimeout, actionOPC, WaitTimeOPC, ConnectTimeout);
            //        }
            //        catch (Exception ex)
            //        {
            //            Log.Error("Error in Print Configuration OPC: " + ex.Message);
            //        }
            //    }
            //    nodelist = nodelist.NextSibling;

            //}
        }

        private void LoadTaglistConfiguration(string Filename)
        {
            var reader = new StreamReader(AppDomain.CurrentDomain.BaseDirectory + Filename);
            var csv = new CsvReader(reader);
            //csv.Configuration.HeaderValidated = 
            //using (var stream as new StreamReader("KMW_AGG2.csv");
            //using (var csv = new CsvReader(reader))
            {
                var records = csv.GetRecords<AggregatorTag>();
                string opcName;
                OPCClient CurrentServer =  null;
                foreach ( AggregatorTag tag in records)
                {
                    opcName = tag.OPCServer;
                    if (OPCClientsList.Count > 0)
                    {
                        CurrentServer = OPCClientsList.Find(x => x.ProgID.Equals(opcName));
                    }
                    if(CurrentServer == null)
                    {
                        CurrentServer = new OPCClient(opcName, ScanningTimer, SQLTimer, SQLServer, SQLUser, SQLPass, SQLDB, SQLTable, SQLCatalog, Log);
                        //CurrentServer.ProgID = opcName;
                        OPCClientsList.Add(CurrentServer);
                    }
                    CurrentServer.AddTag(tag);
                    //Debug.Print("{0}, {1}, {2}, {3}, {4}, {5}, {6}", tag.OPCServer, tag.OPCTag, tag.ScanRate, tag.ServerName, tag.SQL_Format, tag.Transform, tag.WBI_Column);
                }
            }
            reader.Close();



        }

        private void GetAttributesOPC(ref XmlNode nodelist, ref string progid, ref string itemid, ref string staletimeout, ref string actionOPC, ref string waitTimeOPC, ref string ConnectTimeout)
        {
            
        }
    }

}
