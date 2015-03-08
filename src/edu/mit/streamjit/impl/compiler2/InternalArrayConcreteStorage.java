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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;

/**
 * A ConcreteStorage implementation directly addressing its underlying storage
 * and that cannot be adjusted.  As its name suggests, this is most useful for
 * internal storage, where adjusts are not necessary.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 10/10/2013
 */
public class InternalArrayConcreteStorage implements ConcreteStorage {
	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle READ_EXCEPTION_HANDLER = LookupUtils.findStatic(LOOKUP, "readExceptionHandler");
	private static final MethodHandle WRITE_EXCEPTION_HANDLER = LookupUtils.findStatic(LOOKUP, "writeExceptionHandler");
	private final Arrayish array;
	private final MethodHandle readHandle, writeHandle;
	public InternalArrayConcreteStorage(Arrayish array, Storage s) {
		this.array = array;
		int ssc, throughput;
		try {
			ssc = s.steadyStateCapacity();
			throughput = s.throughput();
		} catch (IllegalStateException ex) {
			ssc = throughput = -1;
		}
		String storageInfo = String.format("%s, capacity %d, throughput %d, arraylength %d%nupstream: %s%ndownstream: %s",
				s.id(), ssc, throughput, this.array.size(),
				s.upstreamGroups(),
				s.downstreamGroups());

		this.readHandle = MethodHandles.catchException(array.get(), IndexOutOfBoundsException.class,
				READ_EXCEPTION_HANDLER.bindTo(storageInfo).asType(array.get().type().insertParameterTypes(0, IndexOutOfBoundsException.class)));
		this.writeHandle = MethodHandles.catchException(array.set(), IndexOutOfBoundsException.class,
				WRITE_EXCEPTION_HANDLER.bindTo(storageInfo).asType(array.set().type().insertParameterTypes(0, IndexOutOfBoundsException.class)));
	}

	@Override
	public Class<?> type() {
		return array.type();
	}

	@Override
	public void adjust() {
		throw new AssertionError(String.format("unadjustable! %s.adjust()", this));
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
		throw new AssertionError("don't adjust "+getClass().getSimpleName());
	}

	private static void readExceptionHandler(String storageInfo, IndexOutOfBoundsException ex, int index) {
		throw new AssertionError("reading "+index+": "+storageInfo, ex);
	}

	private static void writeExceptionHandler(String storageInfo, IndexOutOfBoundsException ex, int index, Object data) {
		throw new AssertionError("writing "+data+" at "+index+": "+storageInfo, ex);
	}

	public static StorageFactory factory() {
		return (Storage storage) -> {
			Arrayish array1 = new Arrayish.ArrayArrayish(storage.type(), storage.steadyStateCapacity());
			return new InternalArrayConcreteStorage(array1, storage);
		};
	}

	public static StorageFactory initFactory(final Map<ActorGroup, Integer> initSchedule) {
		return (Storage storage) -> {
			Range<Integer> indices = storage.writeIndexSpan(initSchedule).span(storage.initialDataIndexSpan());
			assert indices.upperBoundType() == BoundType.OPEN;
			int capacity = indices.upperEndpoint();
			Arrayish array1 = new Arrayish.ArrayArrayish(storage.type(), capacity);
			return new InternalArrayConcreteStorage(array1, storage);
		};
	}
}
