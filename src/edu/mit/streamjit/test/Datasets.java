package edu.mit.streamjit.test;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
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

	public static <I> Input<I> lazyInput(Supplier<? extends Input<? extends I>> supplier) {
		final Supplier<? extends Input<? extends I>> memoized = Suppliers.memoize(supplier);
		return InputBufferFactory.wrap(new InputBufferFactory() {
			@Override
			public Buffer createReadableBuffer(int readerMinSize) {
				return unwrap(memoized.get()).createReadableBuffer(readerMinSize);
			}
		});
	}

	public static <I, J> Input<J> transformAll(Function<Input<? super I>, Input<? extends J>> function, Input<I> input) {
		return lazyInput(Suppliers.compose(function, Suppliers.ofInstance(input)));
	}

	public static <I, J> Input<J> transformOne(final Function<? super I, ? extends J> function, final Input<I> input) {
		return InputBufferFactory.wrap(new InputBufferFactory() {
			@Override
			public Buffer createReadableBuffer(final int readerMinSize) {
				return new AbstractReadOnlyBuffer() {
					private final Buffer delegate = unwrap(input).createReadableBuffer(readerMinSize);
					@Override
					@SuppressWarnings("unchecked")
					public Object read() {
						//This is safe because an Input<I>'s buffer contains Is.
						return function.apply((I)delegate.read());
					}
					@Override
					public int size() {
						return delegate.size();
					}
				};
			}
		});
	}
}
