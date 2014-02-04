package edu.mit.streamjit.util.affinity;

import com.google.common.primitives.Longs;
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
	public long getThreadAffinity() {
		Pointer<Long> pmask = null;
		try {
			pmask = Pointer.allocateLong();
			int ret = sched_getaffinity(0, new SizeT(Longs.BYTES), pmask);
			if (ret != 0)
				throw new RuntimeException();
			return pmask.get();
		} finally {
			if (pmask != null) pmask.release();
		}
	}

	@Override
	public void setThreadAffinity(long mask) {
		Pointer<Long> pmask = null;
		try {
			pmask = Pointer.allocateLong();
			pmask.set(mask);
			int ret = sched_setaffinity(0, new SizeT(Longs.BYTES), pmask);
			if (ret != 0)
				throw new RuntimeException();
		} finally {
			if (pmask != null) pmask.release();
		}
	}

	@Override
	public long getProcessAffinity() {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public void setProcessAffinity(long mask) {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public long getMaximalAffinityMask() {
		throw new UnsupportedOperationException("TODO");
	}

	static {
		BridJ.register();
	}
	protected static native int sched_getaffinity(int pid, SizeT cpusetsize, Pointer<Long> mask) throws LastError;
	protected static native int sched_setaffinity(int pid, SizeT cpusetsize, Pointer<Long> mask) throws LastError;
}
