package edu.mit.streamjit.test;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import edu.mit.streamjit.test.Benchmark.Input;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import java.util.Collections;

/**
 * Factories for Benchmark.Input instances.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/14/2013
 */
public final class Inputs {
	private Inputs() {}

	public static Input fromIterable(String name, final Iterable<?> iterable) {
		return new AbstractInput(name) {
			@Override
			public Buffer input() {
				return Buffers.fromIterable(iterable);
			}
		};
	}

	public static Input nCopies(int n, Object o) {
		return fromIterable(o.toString()+" x"+n, Collections.nCopies(n, o));
	}

	public static Input allIntsInRange(int begin, int end) {
		return allIntsInRange(Range.closedOpen(begin, end));
	}

	public static Input allIntsInRange(Range<Integer> range) {
		return fromIterable(range.toString(), ContiguousSet.create(range, DiscreteDomain.integers()));
	}

	private static abstract class AbstractInput implements Input {
		private final String name;
		private AbstractInput(String name) {
			this.name = name;
		}
		@Override
		public Buffer output() {
			return null;
		}
		@Override
		public String toString() {
			return name;
		}
	}
}
