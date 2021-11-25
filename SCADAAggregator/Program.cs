using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace OPCScanner
{
    static class Program
    {   
        //  
        // Internal COM Stuff  
        //  
        ///   
        /// P/Invoke calls  
        ///   
        internal class ComAPI
        {
            [DllImport("OLE32.DLL")]
            public static extern UInt32 CoInitializeSecurity(
             IntPtr securityDescriptor,
             Int32 cAuth,
             IntPtr asAuthSvc,
             IntPtr reserved,
             UInt32 AuthLevel,
             UInt32 ImpLevel,
             IntPtr pAuthList,
             UInt32 Capabilities,
             IntPtr reserved3
             );
            [DllImport("ole32.dll")]
            public static extern UInt32 CoRegisterClassObject(
             ref Guid rclsid,
             [MarshalAs(UnmanagedType.Interface)]IClassFactory pUnkn,
             int dwClsContext,
             int flags,
             out int lpdwRegister);
            [DllImport("ole32.dll")]
            public static extern UInt32 CoRevokeClassObject(int dwRegister);
            public const int RPC_C_AUTHN_LEVEL_PKT_PRIVACY = 6; // Encrypted DCOM communication  
            public const int RPC_C_IMP_LEVEL_IDENTIFY = 2;  // No impersonation really required  
            public const int CLSCTX_LOCAL_SERVER = 4;
            public const int REGCLS_MULTIPLEUSE = 1;
            public const int EOAC_DISABLE_AAA = 0x1000;  // Disable Activate-as-activator  
            public const int EOAC_NO_CUSTOM_MARSHAL = 0x2000; // Disable custom marshalling  
            public const int EOAC_SECURE_REFS = 0x2;   // Enable secure DCOM references  
            public const int CLASS_E_NOAGGREGATION = unchecked((int)0x80040110);
            public const int E_NOINTERFACE = unchecked((int)0x80004002);
            public const string guidIClassFactory = "00000001-0000-0000-C000-000000000046";
            public const string guidIUnknown = "00000000-0000-0000-C000-000000000046";
        }
        ///   
        /// IClassFactory declaration  
        ///   
        [ComImport(), InterfaceType(ComInterfaceType.InterfaceIsIUnknown),
        Guid(ComAPI.guidIClassFactory)]
        internal interface IClassFactory
        {
            [PreserveSig]
            int CreateInstance(IntPtr pUnkOuter, ref Guid riid, out IntPtr ppvObject);
            [PreserveSig]
            int LockServer(bool fLock);
        }
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            
            // Set threading apartment
            // system.Threading.Thread.CurrentThread.ApartmentState = System.Threading.ApartmentState.MTA;
            UInt32 hResult = ComAPI.CoInitializeSecurity(
                IntPtr.Zero, // Add here your Security descriptor  
                -1,
                IntPtr.Zero,
                IntPtr.Zero,
                ComAPI.RPC_C_AUTHN_LEVEL_PKT_PRIVACY,
                ComAPI.RPC_C_IMP_LEVEL_IDENTIFY,
                IntPtr.Zero,
                ComAPI.EOAC_DISABLE_AAA
                | ComAPI.EOAC_SECURE_REFS
                | ComAPI.EOAC_NO_CUSTOM_MARSHAL,
                IntPtr.Zero);


            // This will ensure that future calls to Directory.GetCurrentDirectory()
            // returns the actual executable directory and not something like C:\Windows\System32 
            System.IO.Directory.SetCurrentDirectory(AppDomain.CurrentDomain.BaseDirectory);
#if DEBUG
            try
            {
                OPCScanner myService = new OPCScanner();
                myService.OnDebug();
                System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite);
            }
            catch (Exception)
            {

                
            }
#else
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new OPCScanner()
            };
            ServiceBase.Run(ServicesToRun);
#endif
        }
    }
}
