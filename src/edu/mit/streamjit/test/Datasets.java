package edu.mit.streamjit.test;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.math.IntMath;
import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.blob.AbstractReadOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.test.Benchmark.Dataset;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
		//0 would be valid (an empty input), but would usually be a bug.
		checkArgument(n > 0, "%s must be nonnegative", n);
		if (n == 1) return input;
		return InputBufferFactory.wrap(new InputBufferFactory() {
			@Override
			public Buffer createReadableBuffer(final int readerMinSize) {
				return new AbstractReadOnlyBuffer() {
					private Buffer currentBuffer = InputBufferFactory.unwrap(input).createReadableBuffer(readerMinSize);
					private final int bufferSize = currentBuffer.size();
					private int copiesRemaining = n - 1;
					@Override
					public Object read() {
						if (currentBuffer.size() == 0 && copiesRemaining > 0) {
							currentBuffer = InputBufferFactory.unwrap(input).createReadableBuffer(readerMinSize);
							--copiesRemaining;
						}
						return currentBuffer.read();
					}

					@Override
					public int size() {
						try {
							return IntMath.checkedAdd(currentBuffer.size(), IntMath.checkedMultiply(copiesRemaining, bufferSize));
						} catch (ArithmeticException overflow) {
							return Integer.MAX_VALUE;
						}
					}
				};
			}
		});
	}

	public static <I> Input<I> limit(final int n, final Input<I> input) {
		return InputBufferFactory.wrap(new InputBufferFactory() {
			@Override
			public Buffer createReadableBuffer(final int readerMinSize) {
				return new AbstractReadOnlyBuffer() {
					private final Buffer buffer = InputBufferFactory.unwrap(input).createReadableBuffer(readerMinSize);
					private int size = Math.min(buffer.size(), n);
					@Override
					public Object read() {
						if (size == 0)
							return null;
						--size;
						return buffer.read();
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

	public static <I, J> Input<J> transformAll(Function<? super Input<? super I>, Input<J>> function, Input<I> input) {
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

	/**
	 * TODO: This is extremely slow and generates huge cache files because it's
	 * using Java serialization.
	 * @param <I>
	 * @param supplier
	 * @param filename
	 * @return
	 */
	public static <I> Input<I> fileMemoized(final Supplier<Input<I>> supplier, final String filename) {
		return lazyInput(new Supplier<Input<I>>() {
			@Override
			@SuppressWarnings("unchecked")
			public Input<I> get() {
				Path path = Paths.get(System.getProperty("java.io.tmpdir")).resolve(Paths.get(filename));
				try (FileInputStream fis = new FileInputStream(path.toFile());
						ObjectInputStream ois = new ObjectInputStream(fis)) {
					return Input.fromIterable((Iterable<I>)ois.readObject());
				} catch (IOException | ClassNotFoundException ex) {
					//Not yet cached.
				}

				Input<I> input = supplier.get();
				try {
					Files.deleteIfExists(path);
					Files.createDirectories(path.getParent());
					try (FileOutputStream fos = new FileOutputStream(path.toFile());
							ObjectOutputStream oos = new ObjectOutputStream(fos)) {
						Buffer buf = InputBufferFactory.unwrap(input).createReadableBuffer(42);
						ImmutableList.Builder<Object> builder = ImmutableList.builder();
						while (buf.size() > 0)
							builder.add(buf.read());
						oos.writeObject(builder.build());
					}
				} catch (IOException ex) {
					ex.printStackTrace();
				}

				return input;
			}
		});
	}

	/**
	 * Returns an Input containing the output of the given graph on the given
	 * input when compiled with the given compiler.
	 */
	public static <I, J> Input<J> outputOf(final StreamCompiler sc, final OneToOneElement<I, J> graph, final Input<I> input) {
		return lazyInput(new Supplier<Input<J>>() {
			@Override
			public Input<J> get() {
				List<J> output = new ArrayList<>();
				CompiledStream stream = sc.compile(graph, input, Output.toCollection(output));
				try {
					stream.awaitDrained();
				} catch (InterruptedException ex) {
					throw new RuntimeException(ex);
				}
				return Input.fromIterable(output);
			}
		});
	}
}
