/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.util.affinity;

import edu.mit.streamjit.util.Pair;
import org.bridj.BridJ;
import org.bridj.CRuntime;
import org.bridj.LastError;
import org.bridj.Pointer;
import org.bridj.ann.Convention;
import org.bridj.ann.Library;

/**
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/30/2014
 */
final class WindowsAffinityStrategy implements AffinityStrategy {
	WindowsAffinityStrategy() {}
	@Override
	public long getThreadAffinity() {
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
	public void setThreadAffinity(long mask) {
		long ret = kernel32.SetThreadAffinityMask(kernel32.GetCurrentThread(), mask);
		if (ret == 0)
			throw new RuntimeException();
	}

	@Override
	public long getProcessAffinity() {
		return getProcessAndSystemAffinityMask().first;
	}

	@Override
	public void setProcessAffinity(long mask) {
		int ret = kernel32.SetProcessAffinityMask(kernel32.GetCurrentProcess(), mask);
		if (ret == 0)
			throw new RuntimeException();
	}

	@Override
	public long getMaximalAffinityMask() {
		return getProcessAndSystemAffinityMask().second;
	}

	private Pair<Long, Long> getProcessAndSystemAffinityMask() {
		Pointer<Long> process = null, system = null;
		try {
			process = Pointer.allocateLong();
			system = Pointer.allocateLong();
			int ret = kernel32.GetProcessAffinityMask(kernel32.GetCurrentProcess(), process, system);
			if (ret == 0)
				throw new RuntimeException();
			return Pair.make(process.get(), system.get());
		} finally {
			if (process != null) process.release();
			if (system != null) system.release();
		}
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
		public static native int SetProcessAffinityMask(int hProcess, long dwProcessAffinityMask) throws LastError;
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
