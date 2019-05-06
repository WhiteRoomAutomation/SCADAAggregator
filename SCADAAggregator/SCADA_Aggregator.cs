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

//using System.Runtime.InteropServices;

namespace SCADAAggregator
{
    public class AggregatorTag
    {
        public string OPCServer { get; set; }
        public string OPCTag { get; set; }
        public string Scaling { get; set; }
        public string SQL_Format { get; set; }
        public string ScanRate { get; set; }
        public string ServerName { get; set; }
        public string WBI_Column { get; set; }
        public string Transform { get; set; }
        public List<string> Transforms;

    }

    public class FaultCode
    {
        public string Number { get; set; }
        public string Description { get; set; }
        public string OpStat { get; set; }
    }  

    public partial class SCADA_Aggregator : ServiceBase
    {

        public ILog Log { get; private set; }
        static System.Timers.Timer myTimer = new System.Timers.Timer();
        //static int alarmCounter = 1;
        //static bool exitFlag = false;
        public XmlDocument Config;
        public int EventID = 0;
        public List<OPCClient> OPCClientsList;
        private List<Thread> myThreads;
        public DateTime LastExecuteTime;

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
            // load config settings
            LoadOPCConfiguration();
            CreateThreads();
            
            //OPCClientsList.First().Scan();
            // New thread for each OPC Server.
            // for each server connect to the OPC Server
            // New thread for each OPC Server to havea  10 minute push to SQL
            // Connect to the OPC Server
            // Connect to the SQL Server
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

            LoadTaglistConfiguration("SimulatedTags.csv");
            LoadTurbineNameConfiguration("SimTurbines.txt");

            //LoadMetTaglistConfiguration();
            //LoadMetNameConfiguration();
            LoadFaultListConfiguration();

            // Get Taglist CSV -- Done
            // Get Turbine Name CSV
            // Get MetTaglist CSV
            // Get MetName CSV
            // get FaultList CSV
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
                    //CurrentServer = new opcServer(opcName);
                    //    //CurrentServer.ProgID = opcName;
                    //    OPCClientsList.Add(CurrentServer);
                    //}
                    //CurrentServer.AddTag(tag);
                    //Debug.Print("{0}, {1}, {2}, {3}, {4}, {5}, {6}", tag.OPCServer, tag.OPCTag, tag.ScanRate, tag.ServerName, tag.SQL_Format, tag.Transform, tag.WBI_Column);
                }
            }
            reader.Close();
            //throw new NotImplementedException();
        }

        private void LoadMetNameConfiguration()
        {
            int counter = 0;
            string METName;

            // Read the file and display it line by line.  
            System.IO.StreamReader file =
                new System.IO.StreamReader(AppDomain.CurrentDomain.BaseDirectory + "METNameList.txt");
            while ((METName = file.ReadLine()) != null)
            {
                foreach (OPCClient curServer in OPCClientsList)
                {
                    curServer.AddTurbine(METName);
                }
                //System.Console.WriteLine(line);
                counter++;
            }

            file.Close();
            //throw new NotImplementedException();
        }

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
                        CurrentServer = new OPCClient(opcName);
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
