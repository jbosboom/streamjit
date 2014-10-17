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
package edu.mit.streamjit.impl.compiler2;

import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findGetter;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findStatic;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findVirtual;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;

/**
 * A ConcreteStorage using a circular buffer.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 10/10/2013
 */
public class CircularArrayConcreteStorage implements ConcreteStorage {
	private static final Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle INDEX = findStatic(LOOKUP, "index");
	private static final MethodHandle ADJUST = findVirtual(LOOKUP, "adjust");
	private static final MethodHandle HEAD_GETTER = findGetter(LOOKUP, "head");
	private final Arrayish array;
	private final int capacity, throughput;
	private int head;
	private final MethodHandle readHandle, writeHandle, adjustHandle;
	public CircularArrayConcreteStorage(Arrayish array, Storage s) {
		this.array = array;
		this.capacity = s.steadyStateCapacity();
		assert capacity > 0 : s + " has capacity "+capacity;
		this.throughput = s.throughput();
		this.head = 0;

		MethodHandle index = MethodHandles.insertArguments(INDEX, 0, capacity);
		index = MethodHandles.foldArguments(index, HEAD_GETTER.bindTo(this));
		this.readHandle = MethodHandles.filterArguments(array.get(), 0, index);
		this.writeHandle = MethodHandles.filterArguments(array.set(), 0, index);
		this.adjustHandle = ADJUST.bindTo(this);
	}

	@Override
	public Class<?> type() {
		return array.type();
	}

	@Override
	public void adjust() {
		//head + throughput >= 0 && capacity > 0, so % is mod.
		head = (head + throughput) % capacity;
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
		return writeHandle;
	}

	@Override
	public MethodHandle adjustHandle() {
		return adjustHandle;
	}

	private static int index(int capacity, int head, int physicalIndex) {
		//assumes (physicalIndex + head) >= 0
		//I'd assert but that would add bytes to the method, hampering inlining.
		return (physicalIndex + head) % capacity;
	}

	public static StorageFactory factory() {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				if (storage.steadyStateCapacity() == 0)
					return new EmptyConcreteStorage(storage);
				Arrayish array = storage.type().isPrimitive() ?
						new Arrayish.UnsafeArrayish(storage.type(), storage.steadyStateCapacity()) :
						new Arrayish.ArrayArrayish(storage.type(), storage.steadyStateCapacity());
				return new CircularArrayConcreteStorage(array, storage);
			}
		};
	}
}
