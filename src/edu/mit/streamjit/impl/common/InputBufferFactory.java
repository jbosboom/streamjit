/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.common;

import static com.google.common.base.Preconditions.*;
import com.google.common.reflect.Reflection;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Input.ManualInput;
import edu.mit.streamjit.impl.blob.Buffer;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findConstructor;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findGetter;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findSetter;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.params;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
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
			getInputBufferFactory = findGetter(INPUT_LOOKUP, "input");
			newInput = findConstructor(INPUT_LOOKUP, params(1));
		}
	}
	private static final class ManualInputHolder {
		private static final MethodHandle setManualInputDelegate;
		static {
			Reflection.initialize(ManualInput.class);
			assert MANUALINPUT_LOOKUP != null;
			setManualInputDelegate = findSetter(MANUALINPUT_LOOKUP, "delegate");
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
