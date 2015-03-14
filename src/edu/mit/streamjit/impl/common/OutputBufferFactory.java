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
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.impl.blob.Buffer;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/17/2013
 */
public abstract class OutputBufferFactory {
	public abstract Buffer createWritableBuffer(int writerMinSize);

	public static MethodHandles.Lookup OUTPUT_LOOKUP;
	private static final class OutputHolder {
		private static final MethodHandle getOutputBufferFactory;
		private static final MethodHandle newOutput;
		static {
			Reflection.initialize(Output.class);
			assert OUTPUT_LOOKUP != null;
			try {
				getOutputBufferFactory = OUTPUT_LOOKUP.findGetter(Output.class, "output", OutputBufferFactory.class);
				newOutput = OUTPUT_LOOKUP.findConstructor(Output.class, MethodType.methodType(void.class, OutputBufferFactory.class));
			} catch (NoSuchFieldException | IllegalAccessException | NoSuchMethodException ex) {
				throw new RuntimeException(ex);
			}
		}
	}

	public static <O> Output<O> wrap(OutputBufferFactory output) {
		checkNotNull(output);
		try {
			return (Output<O>)OutputHolder.newOutput.invokeExact(output);
		} catch (Throwable ex) {
			throw new RuntimeException(ex);
		}
	}

	public static OutputBufferFactory unwrap(Output<?> output) {
		checkNotNull(output);
		try {
			return (OutputBufferFactory)OutputHolder.getOutputBufferFactory.invokeExact(output);
		} catch (Throwable ex) {
			throw new RuntimeException(ex);
		}
	}
}
