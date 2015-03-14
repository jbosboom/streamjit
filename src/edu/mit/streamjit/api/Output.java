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
package edu.mit.streamjit.api;

import edu.mit.streamjit.impl.blob.AbstractWriteOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import edu.mit.streamjit.impl.common.OutputBufferFactory;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/17/2013
 */
public class Output<O> {
	static {
		OutputBufferFactory.OUTPUT_LOOKUP = MethodHandles.lookup();
	}
	private final OutputBufferFactory output;
	private Output(OutputBufferFactory output) {
		this.output = output;
	}

	@Override
	public String toString() {
		return output.toString();
	}

	public static final class ManualOutput<O> extends Output<O> {
		//TODO: volatile?
		private volatile Buffer buffer;
		private ManualOutput(OutputBufferFactory output) {
			super(output);
		}
		private static <O> ManualOutput<O> create() {
			class ManualRealOutput extends OutputBufferFactory {
				private ManualOutput<?> manualOutput;
				@Override
				public Buffer createWritableBuffer(int writerMinSize) {
					Buffer buf = Buffers.blockingQueueBuffer(new ArrayBlockingQueue<>(writerMinSize), false, false);
					manualOutput.buffer = buf;
					return buf;
				}
				@Override
				public String toString() {
					return "Output.createManualOutput()";
				}
			}
			ManualRealOutput mro = new ManualRealOutput();
			ManualOutput<O> mo = new ManualOutput<>(mro);
			mro.manualOutput = mo;
			return mo;
		}
		@SuppressWarnings("unchecked")
		public O poll() {
			return (O)buffer.read();
		}
		public int poll(O[] data, int offset, int length) {
			return buffer.read(data, offset, length);
		}
	}

	public static <O> ManualOutput<O> createManualOutput() {
		return ManualOutput.create();
	}

	public static <O> Output<O> blackHole() {
		return new Output<>(new OutputBufferFactory() {
			@Override
			public Buffer createWritableBuffer(final int writerMinSize) {
				return new AbstractWriteOnlyBuffer() {
					@Override
					public boolean write(Object t) {
						return true;
					}
				};
			}
		});
	}

	//TODO: we need flush() for good performance, and close() to avoid leaks.
//	public static <O> Output<O> toBinaryFile(Path path, Class<I> type) {
//
//	}

	public static <O> Output<O> toCollection(final Collection<? super O> coll) {
		return new Output<>(new OutputBufferFactory() {
			@Override
			public Buffer createWritableBuffer(int writerMinSize) {
				return new AbstractWriteOnlyBuffer() {
					@Override
					@SuppressWarnings("unchecked")
					public boolean write(Object t) {
						coll.add((O)t);
						return true;
					}
				};
			}
		});
	}

	public static <O> Output<O> toPrintStream(final PrintStream stream) {
		return new Output<>(new OutputBufferFactory() {
			@Override
			public Buffer createWritableBuffer(int writerMinSize) {
				return new AbstractWriteOnlyBuffer() {
					@Override
					public boolean write(Object t) {
						stream.println(t);
						return true;
					}
				};
			}
		});
	}
}
