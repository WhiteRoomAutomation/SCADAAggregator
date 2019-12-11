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

namespace OPCScanner
{
    public class ScanningTag
    {

        public ScanningTag()
        {
            bAdded = false;
        }

        public void SetAdded(bool bAdd)
        {
            bAdded = bAdd;
        }
        public bool GetAdded()
        {
            return bAdded;
        }
        public string OPC_Tagname { get; set; }
        private bool bAdded;





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

    public partial class OPCScanner : ServiceBase
    {

        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern bool SetServiceStatus(System.IntPtr handle, ref ServiceStatus serviceStatus);

        public static log4net.ILog Log { get; private set; }
        static System.Timers.Timer myTimer = new System.Timers.Timer();
        public XmlDocument Config;
        public int EventID = 0;
        public List<OPCClient> OPCClientsList;
        private List<Thread> myThreads;
        public DateTime LastExecuteTime;

        private int ScanningTimer = 0;
        //private int SQLTimer = 0;
        //private string SQLServer = "";
        //private string SQLUser = "";
        //private string SQLPass = "";
        //private string SQLTable = "";
        //private string SQLFaultTable = "";
        //private string SQLDB = "";
        //private string SQLCatalog = "";

        public OPCScanner()
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
            LoadXMLConfiguration();

            // Add all OPC items before beginning scanning
            foreach (OPCClient myClient in OPCClientsList)
            {
                myClient.InitializeComponent();
                // if not connected then connect and add tags
                if (myClient.IsConnected() == false)
                    {
                        if (myClient.Connect() == true)
                        {
                            myClient.AddOPCItems();

                        }

                    }
            }
            Log.Warn("Configuration loaded and OPC Tags created. Starting item reads.");
            CreateThreads();
            
            serviceStatus.dwCurrentState = ServiceState.SERVICE_RUNNING;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);
        }

        private void CreateThreads()
        {
            Log.Info("Creating threads");
            foreach (OPCClient curServer in OPCClientsList)
            {
                // now that the items are all added we can active groups and start scanning
                curServer.ActivateGroups();
                ThreadStart childref = new ThreadStart(curServer.Scan);
                Thread childThread = new Thread(childref);
                myThreads.Add(childThread);
                childThread.Start();
                
            }
        }

        private void LoadXMLConfiguration()
        {
            Log.Info("Loading XML Configuration");
            Config = new XmlDocument();
            try { Config.Load("OPCScanner.xml"); }
            catch (System.IO.FileNotFoundException)
            {
                Config.LoadXml("<?xml version=\"1.0\"?> \n" +
                "<Configuration> \n" +
                "   <OPCServers> \n" +
                "       <Config Taglist=\"Simtags.csv\" ProgID=\"Matrikon.OPC.Simulation.1\" ScanningTimer=\"1000\"/> \n" +
                "   </OPCServers> \n" +
                "</Configuration>");
            }

            XmlNode configNode = Config.SelectSingleNode("/Configuration");
            XmlNode tempAttribute;
            XmlNode OPCNode = configNode.FirstChild; // get the <OPCServers> node
            XmlNode nodeList = OPCNode.FirstChild;

            //tempAttribute = configNode.Attributes.GetNamedItem("ScanningTimer");
            //if (tempAttribute != null)
            //    ScanningTimer = Convert.ToInt32(tempAttribute.Value);
            //else
            //    ScanningTimer = 10000;
            //tempAttribute = configNode.Attributes.GetNamedItem("SQLTimer");
            //if (tempAttribute != null)
            //    SQLTimer = Convert.ToInt32(tempAttribute.Value);
            //else
            //    SQLTimer = 600000;
            //tempAttribute = configNode.Attributes.GetNamedItem("SQLServer");
            //if (tempAttribute != null)
            //    SQLServer = tempAttribute.Value;
            //else
            //    SQLServer = "127.0.0.1";
            //tempAttribute = configNode.Attributes.GetNamedItem("SQLUser");
            //if (tempAttribute != null)
            //    SQLUser = tempAttribute.Value;
            //else
            //    SQLUser = "sa";
            //tempAttribute = configNode.Attributes.GetNamedItem("SQLPass");
            //if (tempAttribute != null)
            //    SQLPass = tempAttribute.Value;
            //else
            //    SQLPass = "sa";

            //tempAttribute = configNode.Attributes.GetNamedItem("SQLDB");
            //if (tempAttribute != null)
            //    SQLDB = tempAttribute.Value;
            //else
            //    SQLDB = "SQLEXPRESS";

            //tempAttribute = configNode.Attributes.GetNamedItem("SQLCatalog");
            //if (tempAttribute != null)
            //    SQLCatalog = tempAttribute.Value;
            //else
            //    SQLCatalog = "WCS";

            //tempAttribute = configNode.Attributes.GetNamedItem("SQLTable");
            //if (tempAttribute != null)
            //    SQLTable = tempAttribute.Value;
            //else
            //    SQLTable = "";// "turb_ten_min";

            //tempAttribute = configNode.Attributes.GetNamedItem("SQLFaultTable");
            //if (tempAttribute != null)
            //    SQLFaultTable = tempAttribute.Value;
            //else
            //    SQLFaultTable = "";//"turb_fault";
            //LogXMLConfiguration();
            string szText;
            while (nodeList != null)
            {

                tempAttribute = nodeList.Attributes.GetNamedItem("ProgID");
                if (tempAttribute != null)
                {
                    string szOPCServer = tempAttribute.Value;
                    string szHostname;
                    tempAttribute = nodeList.Attributes.GetNamedItem("Hostname");
                    if (tempAttribute != null)
                        szHostname = tempAttribute.Value;
                    else
                        szHostname = null;
                    szText = "Loading OPC Server: " + szOPCServer;
                    Log.Info(szText);

                    tempAttribute = nodeList.Attributes.GetNamedItem("ScanningTimer");
                    if (tempAttribute != null)
                        ScanningTimer = Convert.ToInt32(tempAttribute.Value);
                    else
                        ScanningTimer = 1000;
                    szText = "Scanning Timer: " + ScanningTimer;
                    Log.Info(szText);

                    //tempAttribute = nodeList.Attributes.GetNamedItem("SQLTable");
                    //if (tempAttribute != null)
                    //    szTable = tempAttribute.Value;
                    //else
                    //    szTable = SQLTable;

                    //tempAttribute = nodeList.Attributes.GetNamedItem("SQLFaultTable");
                    //if (tempAttribute != null)
                    //    szFault = tempAttribute.Value;
                    //else
                    //    szFault = SQLFaultTable;

                    //tempAttribute = nodeList.Attributes.GetNamedItem("NameColumn");
                    //if (tempAttribute != null)
                    //    szNameColumn = tempAttribute.Value;
                    //else
                    //    szNameColumn = "TURBINE_NAME";

                    OPCClient CurrentServer = new OPCClient(szOPCServer, szHostname, ScanningTimer, Log);
                    OPCClientsList.Add(CurrentServer);

                    tempAttribute = nodeList.Attributes.GetNamedItem("Taglist");
                    if (tempAttribute != null)
                        LoadTaglistConfiguration(tempAttribute.Value, CurrentServer);

                    //tempAttribute = nodeList.Attributes.GetNamedItem("Turbines");
                    //if (tempAttribute != null)
                    //    LoadTurbineNameConfiguration(tempAttribute.Value, CurrentServer);

                    //tempAttribute = nodeList.Attributes.GetNamedItem("FaultTags");
                    //if (tempAttribute != null)
                    //    LoadFaultTagsConfiguration(tempAttribute.Value, CurrentServer);
                }
                nodeList = nodeList.NextSibling;
            }
        }

        protected override void OnStop()
        {
            // disconnect OPC Server
            foreach (OPCClient curClient in OPCClientsList)
            {
                curClient.bScanning = false;
                curClient.Disconnect();
            }
        }

 

        private OPCClient LoadTaglistConfiguration(string Filename, OPCClient CurrentServer)
        {
            Log.Info("Loading Taglist Configuration");
            try
            {
                var reader = new StreamReader(AppDomain.CurrentDomain.BaseDirectory + Filename);
                var csv = new CsvReader(reader);
                
                csv.Configuration.AllowComments = true;
                try
                {
                    var records = csv.GetRecords<ScanningTag>();
                    foreach (ScanningTag tag in records)
                    {

                        CurrentServer.AddTag(tag);
                        LogTagConfiguration(tag);
                    }
                }
                catch(HeaderValidationException e)
                {
                    Log.Error("CSV Header Validation error: " + e.Message);
                }
                reader.Close();
            }
            catch (System.IO.FileNotFoundException)
            {
                Log.Error("Error Loading Taglist Configuration: file not found");


            }

            return CurrentServer;

        }

        private void LogTagConfiguration(ScanningTag tag)
        {
            string szText;
            szText = "Tagname: " + tag.OPC_Tagname;
            Log.Info(szText);
        }

    }

}
