package edu.mit.streamjit.apps;

import edu.mit.streamjit.apps.Benchmark.Input;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import java.util.Collections;
import java.util.List;

/**
 * Factories for Benchmark.Input instances.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/14/2013
 */
public final class Inputs {
	private Inputs() {}

	public static Input fromList(String name, final List<?> list) {
		return new AbstractInput(name) {
			@Override
			public Buffer input() {
				return Buffers.fromList(list);
			}
		};
	}

	public static Input nCopies(int n, Object o) {
		return fromList(o.toString()+" x"+n, Collections.nCopies(n, o));
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
