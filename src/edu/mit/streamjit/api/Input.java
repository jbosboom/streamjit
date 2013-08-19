package edu.mit.streamjit.api;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import edu.mit.streamjit.impl.blob.AbstractBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
				return new AbstractBuffer() {
					@Override
					public Object read() {
						return null;
					}
					@Override
					public boolean write(Object t) {
						throw new UnsupportedOperationException("read-only buffer");
					}
					@Override
					public int size() {
						return 0;
					}
					@Override
					public int capacity() {
						return Integer.MAX_VALUE;
					}
				};
			}
		});
	}

	private static final ImmutableMap<Class<?>, Integer> SIZE_MAP = ImmutableMap.<Class<?>, Integer>builder()
			.put(Boolean.class, 1)
			.put(Byte.class, 1)
			.put(Short.class, 2)
			.put(Character.class, 2)
			.put(Integer.class, 4)
			.put(Long.class, 8)
			.put(Float.class, 4)
			.put(Double.class, 8)
			.build();
	public static <I> Input<I> fromBinaryFile(Path path, Class<I> type) {
		checkArgument(Primitives.isWrapperType(type) && !type.equals(Void.class), "not a wrapper type: %s", type);
		if (!type.equals(Integer.class))
			throw new UnsupportedOperationException("TODO: only Integer.class supported for now");
		class BinaryFileRealInput extends InputBufferFactory {
			private final Path path;
			private final Class<?> type;
			private final int size;
			private BinaryFileRealInput(Path path, Class<?> type) {
				this.path = path;
				this.type = type;
				this.size = SIZE_MAP.get(type);
			}
			@Override
			public Buffer createReadableBuffer(int readerMinSize) {
				MappedByteBuffer file0 = null;
				try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ)) {
					file0 = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}
				final IntBuffer file = file0.asIntBuffer();
				return new AbstractBuffer() {
					@Override
					public Object read() {
						return file.get();
					}
					@Override
					public boolean write(Object t) {
						throw new UnsupportedOperationException("read-only buffer");
					}
					@Override
					public int size() {
						return file.remaining();
					}
					@Override
					public int capacity() {
						return size();
					}
				};
			}
			@Override
			public String toString(){
				return "Input.fromBinaryFile("+path+", "+type.getSimpleName()+".class)";
			}
		}
		return new Input<>(new BinaryFileRealInput(path, type));
	}
}
