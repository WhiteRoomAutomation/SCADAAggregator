using Common.Logging;
using log4net;
using log4net.Config;

namespace OPCScanner
{
    partial class OPCScanner
    {
        /// <summary> 
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            components = new System.ComponentModel.Container();
            this.ServiceName = "OPCScanner";
            OPCClientsList = new System.Collections.Generic.List<OPCClient>();
            myThreads = new System.Collections.Generic.List<System.Threading.Thread>();
            Log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
            XmlConfigurator.Configure(new System.IO.FileInfo(System.AppDomain.CurrentDomain.BaseDirectory + "log4netSettings.config"));
        }

        #endregion
    }
}
