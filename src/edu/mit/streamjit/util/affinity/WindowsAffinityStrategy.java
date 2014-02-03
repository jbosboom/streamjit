package edu.mit.streamjit.util.affinity;

import org.bridj.BridJ;
import org.bridj.CRuntime;
import org.bridj.LastError;
import org.bridj.Pointer;
import org.bridj.ann.Convention;
import org.bridj.ann.Library;

/**
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/30/2014
 */
final class WindowsAffinityStrategy implements AffinityStrategy {
	WindowsAffinityStrategy() {}
	@Override
	public long get() {
		Pointer<Long> tbi = null;
		try {
			tbi = Pointer.allocateLongs(ntdll.QUADWORD_SIZEOF_THREAD_BASIC_INFORMATION);
			int ret = ntdll.NtQueryInformationThread(kernel32.GetCurrentThread(), ntdll.THREAD_BASIC_INFORMATION_CLASS, tbi, tbi.getValidBytes(), Pointer.NULL);
			if (!ntdll.NT_SUCCESS(ret))
				throw new RuntimeException(Integer.toHexString(ret));
			return tbi.get(ntdll.AFFINITY_MASK_OFFSET_THREAD_BASIC_INFORMATION);
		} finally {
			if (tbi != null) tbi.release();
		}
	}

	@Override
	public void set(long mask) {
		long ret = kernel32.SetThreadAffinityMask(kernel32.GetCurrentThread(), mask);
		if (ret == 0)
			throw new RuntimeException();
	}

	@Library("kernel32")
	@org.bridj.ann.Runtime(CRuntime.class)
	private static final class kernel32 {
		static {
			BridJ.register();
		}
		@Convention(Convention.Style.StdCall)
		public static native int GetProcessAffinityMask(int hProcess, Pointer<Long> lpProcessAffinityMask, Pointer<Long> lpSystemAffinityMask) throws LastError;
		@Convention(Convention.Style.StdCall)
		public static native long SetThreadAffinityMask(int hThread, long lpThreadAffinityMask) throws LastError;
		@Convention(Convention.Style.StdCall)
		public static native int GetCurrentProcess();
		@Convention(Convention.Style.StdCall)
		public static native int GetCurrentThread();
	}

	@Library("ntdll")
	@org.bridj.ann.Runtime(CRuntime.class)
	private static final class ntdll {
		static {
			BridJ.register();
		}
		/* from the _THREAD_INFORMATION_CLASS enum */
		public static final int THREAD_BASIC_INFORMATION_CLASS = 0;
		//TODO: these may change on 32-bit machines, if there are any left.
		public static final int QUADWORD_SIZEOF_THREAD_BASIC_INFORMATION = 6;
		public static final int AFFINITY_MASK_OFFSET_THREAD_BASIC_INFORMATION = 4;
		@Convention(Convention.Style.StdCall)
		public static native int NtQueryInformationThread(int ThreadHandle, int ThreadInformationClass, Pointer<?> ThreadInformation, long ThreadInformationLength, /* optional */ Pointer<Long> ReturnLength);

		public static boolean NT_SUCCESS(int ntstatus) {
			return 0 <= ntstatus && ntstatus <= 0x3FFFFFFF;
		}
	}
}
