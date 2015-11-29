using System;
using System.Runtime.InteropServices;

namespace Dataflow.Caching
{
    [System.Security.SuppressUnmanagedCodeSecurity()]
    static unsafe class NativeOS
    {
        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern void* VirtualAlloc(void* address, uint numBytes, uint commitOrReserve, uint pageProtectionMode);
        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern unsafe bool VirtualFree(void* address, uint numBytes, int pageFreeMode);
        [Flags]
        internal enum TokenAccess
        {
            AssignPrimary = 0x1, Duplicate = 0x2, Impersonate = 0x4, Query = 0x8, QuerySource = 0x10,
            AdjustPrivileges = 0x20, AdjustGroups = 0x40, AdjustDefault = 0x80, AdjustSessionId = 0x100,
            Read = 0x00020000 | Query,
            Write = 0x00020000 | AdjustPrivileges | AdjustGroups | AdjustDefault,
            AllAccess = 0x000F01FF,
            MaximumAllowed = 0x02000000
        }
        internal enum SecurityImpersonationLevel { Anonymous = 0, Identification = 1, Impersonation = 2, Delegation = 3, }
        internal enum TokenType { Primary = 1, Impersonation = 2, }
        internal const uint SE_PRIVILEGE_DISABLED = 0x0, SE_PRIVILEGE_ENABLED = 0x2;
        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        internal struct LUID { internal uint LowPart; internal uint HighPart; }
        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        internal struct LUID_AND_ATTRIBUTES { internal LUID Luid; internal uint Attributes; }
        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        internal struct TOKEN_PRIVILEGE { internal uint PrivilegeCount; internal LUID_AND_ATTRIBUTES Privilege; }

        //internal const int ERROR_SUCCESS = 0x0, ERROR_ACCESS_DENIED = 0x5, ERROR_NOT_ENOUGH_MEMORY = 0x8;
        //internal const int ERROR_NO_TOKEN = 0x3f0, ERROR_NOT_ALL_ASSIGNED = 0x514, ERROR_NO_SUCH_PRIVILEGE = 0x521;
        //internal const int ERROR_CANT_OPEN_ANONYMOUS = 0x543;

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        internal static extern bool AdjustTokenPrivileges(IntPtr handle, bool DisableAllPrivileges, ref TOKEN_PRIVILEGE NewState, uint BufferLength, TOKEN_PRIVILEGE* PreviousState, uint* ReturnLength);
        [DllImport("advapi32.dll", EntryPoint = "LookupPrivilegeValueW", CharSet = CharSet.Auto, SetLastError = true)]
        internal static extern bool LookupPrivilegeValue(string lpSystemName, string lpName, ref LUID Luid);
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern void CloseHandle(IntPtr handle);
        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern IntPtr GetCurrentProcess();
        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern uint GetLargePageMinimum();
        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        internal static extern bool OpenProcessToken(IntPtr ProcessToken, TokenAccess DesiredAccess, IntPtr* TokenHandle);

        public static bool SetPrivilege(string name, bool enable)
        {
            IntPtr token = IntPtr.Zero;
            TOKEN_PRIVILEGE tp = new TOKEN_PRIVILEGE();
            bool status = false;
            try
            {
                OpenProcessToken(GetCurrentProcess(), TokenAccess.AdjustPrivileges | TokenAccess.Query, &token);
                LookupPrivilegeValue(null, name, ref tp.Privilege.Luid);
                tp.PrivilegeCount = 1;
                tp.Privilege.Attributes = enable ? SE_PRIVILEGE_ENABLED : 0;
                status = AdjustTokenPrivileges(token, false, ref tp, 0, null, null);
                if (status && Marshal.GetLastWin32Error() != 0)
                    status = false;
            }
            finally
            {
                CloseHandle(token);
            }
            return status;
        }
    }

    public class WindowsVirtualMemory : LocalCache.IVirtualMemory
    {
        private uint _large_page_size;

        public WindowsVirtualMemory(bool use_large_pages = false)
        {
            if (use_large_pages)
            {
                if (NativeOS.SetPrivilege("SeLockMemoryPrivilege", true))
                    _large_page_size = NativeOS.GetLargePageMinimum();
            }
        }

        public uint PageSize()
        {
            return _large_page_size == 0 ? 0x10000 : _large_page_size;
        }

        public unsafe void* Alloc(uint numBytes, out uint actualBytes)
        {
            var commit_flags = 0x00001000 | (_large_page_size == 0 ? 0x00002000 : 0x20000000);
            var page_size = PageSize();
            actualBytes = (numBytes + page_size - 1) & ~(page_size - 1);
            var data = NativeOS.VirtualAlloc(null, actualBytes, (uint)commit_flags, 0x4); //-READ|WRITE
            if (data != null && _large_page_size != 0)
            {
                _large_page_size = 0;
                return Alloc(numBytes, out actualBytes);
            }
            return data; 
        }

        public unsafe bool Free(void* address, uint bytes)
        {
            return NativeOS.VirtualFree(address, bytes, 0x8000); //-- RELEASE.
        }

    }

}
