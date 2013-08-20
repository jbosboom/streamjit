package edu.mit.streamjit.test;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import edu.mit.streamjit.test.Benchmark.Dataset;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import java.util.Collections;

/**
 * Factories for Benchmark.Dataset instances.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/14/2013
 */
public final class Datasets {
	private Datasets() {}

	public static Dataset fromIterable(String name, final Iterable<?> iterable) {
		return Dataset.builder().name(name).input(InputBufferFactory.wrap(new InputBufferFactory() {
			@Override
			public Buffer createReadableBuffer(int readerMinSize) {
				return Buffers.fromIterable(iterable);
			}
		})).build();
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
}
