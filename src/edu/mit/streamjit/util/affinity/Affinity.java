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

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.bridj.Platform;

/**
 * Provides static methods for getting and setting thread or process CPU
 * affinity.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/29/2014
 */
public final class Affinity {
	private static final AffinityStrategy STRATEGY;
	static {
		if (Platform.isWindows())
			STRATEGY = new WindowsAffinityStrategy();
		else if (Platform.isMacOSX())
			//OSX doesn't support thread affinity at all.
			STRATEGY = new NullAffinityStrategy();
		else if (Platform.isUnix())
			STRATEGY = new LinuxAffinityStrategy();
		else
			STRATEGY = new NullAffinityStrategy();
	}

	public static ImmutableSet<Integer> getThreadAffinity() {
		return expand(STRATEGY.getThreadAffinity());
	}
	public static void setThreadAffinity(Set<Integer> cpus) {
		STRATEGY.setThreadAffinity(contract(cpus));
	}
	public static ImmutableSet<Integer> getProcessAffinity() {
		return expand(STRATEGY.getProcessAffinity());
	}
	public static void setProcessAffinity(Set<Integer> cpus) {
		STRATEGY.setProcessAffinity(contract(cpus));
	}
	public static ImmutableSet<Integer> getMaximalAffinity() {
		return expand(STRATEGY.getMaximalAffinityMask());
	}

	private static ImmutableSet<Integer> expand(long mask) {
		ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();
		for (int i = 0; i < Long.SIZE && mask != 0; ++i) {
			if ((mask & 1) != 0)
				builder.add(i);
			mask >>>= 1;
		}
		return builder.build();
	}

	private static long contract(Set<Integer> cpus) {
		long mask = 0;
		for (int c : cpus)
			mask |= (1L << c);
		return mask;
	}

	public static void main(String[] args) {
		System.out.println(getThreadAffinity());
		setThreadAffinity(ImmutableSet.of(2));
		System.out.println(getThreadAffinity());
	}
}
