/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
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
package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.PeekableBuffer;
import edu.mit.streamjit.util.bytecode.methodhandles.Combinators;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findVirtual;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;

/**
 * A read-only ConcreteStorage implementation wrapping an PeekableBuffer.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 2/18/2014
 */
public final class PeekableBufferConcreteStorage implements ConcreteStorage {
	private static final MethodHandle IRB_PEEK = findVirtual(PeekableBuffer.class, "peek");
	private static final MethodHandle IRB_CONSUME = findVirtual(PeekableBuffer.class, "consume");
	private final Class<?> type;
	private final int throughput, minReadIndex;
	private final PeekableBuffer buffer;
	private final MethodHandle readHandle, adjustHandle;
	public PeekableBufferConcreteStorage(Class<?> type, int throughput, int minReadIndex, PeekableBuffer buffer) {
		this.type = type;
		this.throughput = throughput;
		this.minReadIndex = minReadIndex;
		this.buffer = buffer;
		this.readHandle = MethodHandles.filterArguments(IRB_PEEK.bindTo(buffer), 0, Combinators.adder(-minReadIndex));
		this.adjustHandle = MethodHandles.insertArguments(IRB_CONSUME, 0, buffer, throughput);
	}

	@Override
	public Class<?> type() {
		return type;
	}

	@Override
	public void write(int index, Object data) {
		throw new AssertionError(String.format("read-only! %s.write(%d, %s)", this, index, data));
	}

	@Override
	public void adjust() {
		try {
			adjustHandle.invokeExact();
		} catch (Throwable ex) {
			throw new AssertionError(String.format("%s.adjust()", this), ex);
		}
	}

	@Override
	public void sync() {
	}

	@Override
	public MethodHandle readHandle() {
		return readHandle;
	}

	@Override
	public MethodHandle writeHandle() {
		throw new UnsupportedOperationException("read-only");
	}

	@Override
	public MethodHandle adjustHandle() {
		return adjustHandle;
	}

	public PeekableBuffer buffer() {
		return buffer;
	}

	public int minReadIndex() {
		return minReadIndex;
	}

	public static StorageFactory factory(final Map<Token, PeekableBuffer> buffers) {
		return (Storage storage) -> {
			assert buffers.containsKey(storage.id()) : storage.id()+" not in "+buffers;
			//Hack: we don't have the throughput when making init storage,
			//but we don't need it either.
			int throughput1;
			try {
				throughput1 = storage.throughput();
			} catch (IllegalStateException ignored) {
				throughput1 = Integer.MIN_VALUE;
			}
			ImmutableSet<ActorGroup> relevantGroups = ImmutableSet.<ActorGroup>builder()
					.addAll(storage.upstreamGroups()).addAll(storage.downstreamGroups()).build();
			ImmutableSortedSet<Integer> readIndices = storage.readIndices(Maps.asMap(relevantGroups, i -> 1));
			int minReadIndex1 = readIndices.isEmpty() ? Integer.MIN_VALUE : readIndices.first();
			return new PeekableBufferConcreteStorage(storage.type(), throughput1, minReadIndex1, buffers.get(storage.id()));
		};
	}
}
