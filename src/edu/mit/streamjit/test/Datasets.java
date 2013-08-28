package edu.mit.streamjit.test;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.impl.blob.AbstractReadOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.test.Benchmark.Dataset;
import java.util.Collections;

/**
 * Factories for Benchmark.Dataset instances.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/14/2013
 */
public final class Datasets {
	private Datasets() {}

	public static Dataset fromIterable(String name, final Iterable<?> iterable) {
		return new Dataset(name, Input.fromIterable(iterable));
	}

	public static Dataset nCopies(int n, Object o) {
		return fromIterable(o.toString()+" x"+n, Collections.nCopies(n, o));
	}

	public static Dataset allIntsInRange(int begin, int end) {
		return allIntsInRange(Range.closedOpen(begin, end));
	}

	public static Dataset allIntsInRange(Range<Integer> range) {
		return fromIterable(range.toString(), ContiguousSet.create(range, DiscreteDomain.integers()));
	}

	public static <I> Input<I> nCopies(final int n, final Input<I> input) {
		return InputBufferFactory.wrap(new InputBufferFactory() {
			@Override
			public Buffer createReadableBuffer(int readerMinSize) {
				final Buffer first = InputBufferFactory.unwrap(input).createReadableBuffer(readerMinSize);
				final int initialSize = first.size()*n;
				return new AbstractReadOnlyBuffer() {
					private Buffer currentBuffer = first;
					private int size = initialSize;
					@Override
					public Object read() {
						if (size == 0)
							return null;
						if (currentBuffer.size() == 0)
							currentBuffer = InputBufferFactory.unwrap(input).createReadableBuffer(initialSize);
						--size;
						return currentBuffer.read();
					}
					@Override
					public int size() {
						return size;
					}
				};
			}
		});
	}
}
