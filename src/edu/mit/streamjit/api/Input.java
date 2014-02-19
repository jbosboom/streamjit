package edu.mit.streamjit.api;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;
import edu.mit.streamjit.impl.blob.AbstractReadOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import edu.mit.streamjit.impl.blob.PeekableBuffer;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.impl.common.NIOBuffers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * A source of input to a stream graph.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/16/2013
 */
public class Input<I> {
	static {
		InputBufferFactory.INPUT_LOOKUP = MethodHandles.lookup();
	}
	private final InputBufferFactory input;
	private Input(InputBufferFactory input) {
		this.input = input;
	}

	@Override
	public String toString() {
		return input.toString();
	}

	public static final class ManualInput<I> extends Input<I> {
		static {
			InputBufferFactory.MANUALINPUT_LOOKUP = MethodHandles.lookup();
		}
		//TODO: Reason about whether this needs to be volatile or not.
		private volatile InputBufferFactory.ManualInputDelegate<I> delegate;
		private ManualInput(InputBufferFactory input) {
			super(input);
		}
		private static <I> ManualInput<I> create() {
			class ManualRealInput extends InputBufferFactory {
				private ManualInput<?> manualInput;
				@Override
				public Buffer createReadableBuffer(int readerMinSize) {
					return Buffers.blockingQueueBuffer(new ArrayBlockingQueue<>(readerMinSize), false, false);
				}
				@Override
				public String toString() {
					return "Input.createManualInput()";
				}
			}
			ManualRealInput mri = new ManualRealInput();
			ManualInput<I> mi = new ManualInput<>(mri);
			mri.manualInput = mi;
			return mi;
		}
		public boolean offer(I t) {
			return delegate.offer(t);
		}
		public int offer(I[] data, int offset, int length) {
			return delegate.offer(data, offset, length);
		}
		public void drain() {
			delegate.drain();
		}
	}

	public static <I> ManualInput<I> createManualInput() {
		return ManualInput.create();
	}

	public static <I> Input<I> empty() {
		return new Input<>(new InputBufferFactory() {
			@Override
			public Buffer createReadableBuffer(int readerMinSize) {
				return new AbstractReadOnlyBuffer() {
					@Override
					public Object read() {
						return null;
					}
					@Override
					public int size() {
						return 0;
					}
				};
			}
		});
	}

	public static <I> Input<I> fromBinaryFile(Path path, Class<I> type, ByteOrder byteOrder) {
		checkArgument(Primitives.isWrapperType(type) && !type.equals(Void.class), "not a wrapper type: %s", type);
		class BinaryFileRealInput extends InputBufferFactory {
			private final Path path;
			private final Class<?> type;
			private final ByteOrder byteOrder;
			private BinaryFileRealInput(Path path, Class<?> type, ByteOrder byteOrder) {
				this.path = path;
				this.type = type;
				this.byteOrder = byteOrder;
			}
			@Override
			public Buffer createReadableBuffer(int readerMinSize) {
				MappedByteBuffer file = null;
				try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ)) {
					file = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}
				file.order(byteOrder);
				return NIOBuffers.wrap(file, type);
			}
			@Override
			public String toString(){
				return "Input.fromBinaryFile("+path+", "+type.getSimpleName()+".class, "+byteOrder+")";
			}
		}
		return new Input<>(new BinaryFileRealInput(path, type, byteOrder));
	}

	/**
	 * Creates an Input containing the elements in the given Iterable.
	 * <p/>
	 * If the iterable is modified while the Input is alive (even if it is not
	 * actively being used), the behavior is undefined. (TODO: Iterables that
	 * modify themselves, e.g., Guava Iterables.consumingIterable?)
	 * <p/>
	 * The returned Input does not remove elements from the iterable as they are
	 * consumed by the stream, so it may be used with multiple streams
	 * successively. Assuming the iterable permits multiple concurrent
	 * iterations, it may be used with multiple streams concurrently.
	 * <p/>
	 * The Input produced by this method requires the iterable's size. If the
	 * iterable is a Collection, its size() method will be used; otherwise a
	 * full iteration will be performed to find the size. (TODO: add an overload
	 * taking the size as parameter? what about indeterminate iterables possibly
	 * requiring buffering?)
	 * @param <I> the type of Input to create
	 * @param iterable the iterable
	 * @return an Input containing the elements in the given iterable
	 */
	public static <I> Input<I> fromIterable(final Iterable<? extends I> iterable) {
		if (iterable instanceof List && iterable instanceof RandomAccess)
			return new Input<>(new RandomAccessListInput((List<?>)iterable));
		return new Input<>(new InputBufferFactory() {
			private final int size = Iterables.size(iterable);
			@Override
			public Buffer createReadableBuffer(int readerMinSize) {
				return Input.fromIterator(iterable.iterator(), size).input.createReadableBuffer(readerMinSize);
			}
		});
	}

	private static final class RandomAccessListInput extends InputBufferFactory {
		private final List<?> list;
		private RandomAccessListInput(List<?> list) {
			assert list instanceof RandomAccess;
			this.list = list;
		}
		@Override
		public PeekableBuffer createReadableBuffer(int readerMinSize) {
			class RandomAccessListBuffer extends AbstractReadOnlyBuffer implements PeekableBuffer {
				private final List<?> list;
				private int base = 0;
				private RandomAccessListBuffer(List<?> list) {
					this.list = list;
				}
				@Override
				public Object read() {
					if (size() <= 0) return null;
					return list.get(base++);
				}
				@Override
				public int size() {
					return list.size() - base;
				}
				@Override
				public Object peek(int index) {
					return list.get(base + index);
				}
				@Override
				public void consume(int items) {
					int size = size();
					if (items > size)
						throw new IndexOutOfBoundsException("consuming "+items+" items when only "+size+" remain");
					base += items;
				}
			}
			return new RandomAccessListBuffer(list);
		}
	}

	/**
	 * Creates an Input containing the elements in the given Iterator.
	 * <p/>
	 * Only size elements will be returned, even if the iterator has more
	 * elements. (If the iterator has fewer elements, a NoSuchElementException
	 * will be thrown in the stream.)
	 * <p/>
	 * The returned Input does not remove elements from the iterator as they are
	 * consumed.
	 * <p/>
	 * Because iterators cannot be reset, the returned Input can only be used
	 * once.
	 * @param <I> the type of Input to create
	 * @param iterator the iterator
	 * @param size the number of elements in the given iterator
	 * @return an Input containing the elements in the given iterator
	 */
	public static <I> Input<I> fromIterator(final Iterator<? extends I> iterator, final int size) {
		return new Input<>(new InputBufferFactory() {
			@Override
			public Buffer createReadableBuffer(int readerMinSize) {
				return new AbstractReadOnlyBuffer() {
					private int remainingSize = size;
					@Override
					public Object read() {
						if (remainingSize <= 0)
							return null;
						--remainingSize;
						return iterator.next();
					}
					@Override
					public int size() {
						return remainingSize;
					}
				};
			}
		});
	}
}
