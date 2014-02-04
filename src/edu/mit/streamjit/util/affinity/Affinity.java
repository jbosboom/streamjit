package edu.mit.streamjit.util.affinity;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.bridj.Platform;

/**
 * Provides static methods for getting and setting thread or process CPU
 * affinity.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/29/2014
 */
public final class Affinity {
	private static final AffinityStrategy STRATEGY;
	static {
		if (Platform.isWindows())
			STRATEGY = new WindowsAffinityStrategy();
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
