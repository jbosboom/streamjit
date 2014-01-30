package edu.mit.streamjit.util.affinity;

import org.bridj.BridJ;
import org.bridj.CRuntime;
import org.bridj.LastError;
import org.bridj.Pointer;
import org.bridj.SizeT;
import org.bridj.ann.Library;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/30/2014
 */
@Library("c")
@org.bridj.ann.Runtime(CRuntime.class)
final class LinuxAffinityStrategy implements AffinityStrategy {
	LinuxAffinityStrategy() {}
	@Override
	public long get() {
		Pointer<Long> pmask = null;
		try {
			pmask = Pointer.allocateLong();
			int ret = sched_getaffinity(0, new SizeT(Long.BYTES), pmask);
			if (ret != 0)
				throw new RuntimeException();
			return pmask.get();
		} finally {
			if (pmask != null) pmask.release();
		}
	}

	@Override
	public void set(long mask) {
		Pointer<Long> pmask = null;
		try {
			pmask = Pointer.allocateLong();
			pmask.set(mask);
			int ret = sched_setaffinity(0, new SizeT(Long.BYTES), pmask);
			if (ret != 0)
				throw new RuntimeException();
		} finally {
			if (pmask != null) pmask.release();
		}
	}

	static {
		BridJ.register();
	}
	protected static native int sched_getaffinity(int pid, SizeT cpusetsize, Pointer<Long> mask) throws LastError;
	protected static native int sched_setaffinity(int pid, SizeT cpusetsize, Pointer<Long> mask) throws LastError;
}
