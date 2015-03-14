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

import com.google.common.primitives.Longs;
import org.bridj.BridJ;
import org.bridj.CRuntime;
import org.bridj.LastError;
import org.bridj.Pointer;
import org.bridj.SizeT;
import org.bridj.ann.Library;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
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
		return new NullAffinityStrategy().getProcessAffinity();
	}

	@Override
	public void setProcessAffinity(long mask) {
		new NullAffinityStrategy().setProcessAffinity(mask);
	}

	@Override
	public long getMaximalAffinityMask() {
		return new NullAffinityStrategy().getMaximalAffinityMask();
	}

	static {
		BridJ.register();
	}
	protected static native int sched_getaffinity(int pid, SizeT cpusetsize, Pointer<Long> mask) throws LastError;
	protected static native int sched_setaffinity(int pid, SizeT cpusetsize, Pointer<Long> mask) throws LastError;
}
