package edu.mit.streamjit.impl.common;

import static com.google.common.base.Preconditions.*;
import com.google.common.reflect.Reflection;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Input.ManualInput;
import edu.mit.streamjit.impl.blob.Buffer;
import static edu.mit.streamjit.util.LookupUtils.findConstructor;
import static edu.mit.streamjit.util.LookupUtils.findGetter;
import static edu.mit.streamjit.util.LookupUtils.findSetter;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/16/2013
 */
public abstract class InputBufferFactory {
	/**
	 * Returns a new Buffer containing some input, or throws
	 * IllegalStateException if the InputBufferFactory instance can only produce a
	 * single buffer.  The returned buffer must be readable, but need not be
	 * read-only.
	 * @return a new Buffer
	 * @throws IllegalStateException if only one buffer can be produced and that
	 * buffer was previously returned
	 */
	public abstract Buffer createReadableBuffer(int readerMinSize);

	public interface ManualInputDelegate<I> {
		public boolean offer(I input);
		public int offer(I[] input, int offset, int length);
		public void drain();
	}

	public static abstract class AbstractManualInputDelegate<I> implements ManualInputDelegate<I> {
		private final Buffer buffer;
		public AbstractManualInputDelegate(Buffer buffer) {
			this.buffer = buffer;
		}
		@Override
		public boolean offer(I input) {
			return buffer.write(input);
		}
		@Override
		public int offer(I[] input, int offset, int length) {
			return buffer.write(input, offset, length);
		}
	}

	public static MethodHandles.Lookup INPUT_LOOKUP, MANUALINPUT_LOOKUP;
	private static final class InputHolder {
		private static final MethodHandle getInputBufferFactory;
		private static final MethodHandle newInput;
		static {
			Reflection.initialize(Input.class);
			assert INPUT_LOOKUP != null;
			getInputBufferFactory = findGetter(INPUT_LOOKUP, Input.class, "input", InputBufferFactory.class);
			newInput = findConstructor(INPUT_LOOKUP, Input.class, InputBufferFactory.class);
		}
	}
	private static final class ManualInputHolder {
		private static final MethodHandle setManualInputDelegate;
		static {
			Reflection.initialize(ManualInput.class);
			assert MANUALINPUT_LOOKUP != null;
			setManualInputDelegate = findSetter(MANUALINPUT_LOOKUP, ManualInput.class, "delegate", ManualInputDelegate.class);
		}
	}

	public static <I> Input<I> wrap(InputBufferFactory input) {
		checkNotNull(input);
		try {
			return (Input<I>)InputHolder.newInput.invokeExact(input);
		} catch (Throwable ex) {
			throw new RuntimeException(ex);
		}
	}

	public static InputBufferFactory unwrap(Input<?> input) {
		checkNotNull(input);
		try {
			return (InputBufferFactory)InputHolder.getInputBufferFactory.invokeExact(input);
		} catch (Throwable ex) {
			throw new RuntimeException(ex);
		}
	}

	public static <I> void setManualInputDelegate(ManualInput<I> input, ManualInputDelegate<I> delegate) {
		checkNotNull(input);
		checkNotNull(delegate);
		try {
			ManualInputHolder.setManualInputDelegate.invokeExact(input, delegate);
		} catch (Throwable ex) {
			throw new RuntimeException(ex);
		}
	}
}
