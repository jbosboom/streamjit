package edu.mit.streamjit.util.affinity;

import org.bridj.BridJ;
import org.bridj.CRuntime;
import org.bridj.LastError;
import org.bridj.Pointer;
import org.bridj.ann.Convention;
import org.bridj.ann.Library;

/**
 * TODO: There's no GetThreadAffinityMask function, but we can use the
 * undocumented NtQueryInformationThread with a THREAD_BASIC_INFORMATION struct.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/30/2014
 */
@Library("kernel32")
@org.bridj.ann.Runtime(CRuntime.class)
final class WindowsAffinityStrategy implements AffinityStrategy {
	WindowsAffinityStrategy() {}
	@Override
	public long get() {
		Pointer<Long> process = null, system = null;
		try {
			process = Pointer.allocateLong();
			system = Pointer.allocateLong();
			int ret = GetProcessAffinityMask(GetCurrentProcess(), process, system);
			if (ret == 0)
				throw new RuntimeException();
			return process.get();
		} finally {
			if (system != null) system.release();
			if (process != null) process.release();
		}
	}

	@Override
	public void set(long mask) {
		long ret = SetThreadAffinityMask(GetCurrentThread(), mask);
		if (ret == 0)
			throw new RuntimeException();
	}

	static {
		BridJ.register();
	}
	@Convention(Convention.Style.StdCall)
	protected native int GetProcessAffinityMask(int hProcess, Pointer<Long> lpProcessAffinityMask, Pointer<Long> lpSystemAffinityMask) throws LastError;
	@Convention(Convention.Style.StdCall)
	protected native long SetThreadAffinityMask(int hThread, long lpThreadAffinityMask) throws LastError;
	@Convention(Convention.Style.StdCall)
	protected native int GetCurrentProcess();
	@Convention(Convention.Style.StdCall)
	protected native int GetCurrentThread();
}
