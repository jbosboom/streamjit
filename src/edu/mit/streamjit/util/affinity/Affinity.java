package edu.mit.streamjit.util.affinity;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

/**
 * Provides static methods for getting and setting the CPU affinity of the
 * current thread.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/29/2014
 */
public final class Affinity {
	private static final AffinityStrategy STRATEGY;
	static {
		if (System.getProperty("os.name").contains("Windows"))
			STRATEGY = new NullAffinityStrategy();
		else
			STRATEGY = new LinuxAffinityStrategy();
	}

	public static ImmutableSet<Integer> get() {
		return expand(STRATEGY.get());
	}
	public static void set(Set<Integer> cpus) {
		STRATEGY.set(contract(cpus));
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
		System.out.println(get());
		set(ImmutableSet.of(2));
		System.out.println(get());
	}
}
