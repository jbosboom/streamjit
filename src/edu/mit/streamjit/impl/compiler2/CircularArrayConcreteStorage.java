package edu.mit.streamjit.impl.compiler2;

import com.google.common.math.IntMath;
import static edu.mit.streamjit.util.LookupUtils.findVirtual;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Array;

/**
 * A ConcreteStorage backed by a circular array.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/10/2013
 */
public class CircularArrayConcreteStorage implements ConcreteStorage {
	private static final Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle INDEX = findVirtual(LOOKUP, CircularArrayConcreteStorage.class, "index", int.class, int.class);
	private static final MethodHandle ADJUST = findVirtual(LOOKUP, CircularArrayConcreteStorage.class, "adjust", void.class);;
	private final Object array;
	private final int capacity, throughput;
	private int head;
	private final MethodHandle readHandle, writeHandle, adjustHandle;
	public CircularArrayConcreteStorage(Storage s) {
		this.capacity = s.steadyStateCapacity();
		this.array = Array.newInstance(s.type(), capacity);
		this.throughput = s.throughput();
		this.head = 0;

		MethodHandle index = INDEX.bindTo(this);
		MethodHandle arrayGetter = MethodHandles.arrayElementGetter(array.getClass()).bindTo(array);
		this.readHandle = MethodHandles.filterArguments(arrayGetter, 0, index);
		MethodHandle arraySetter = MethodHandles.arrayElementSetter(array.getClass()).bindTo(array);
		this.writeHandle = MethodHandles.filterArguments(arraySetter, 0, index);
		this.adjustHandle = ADJUST.bindTo(this);
	}

	@Override
	public Class<?> type() {
		return array.getClass().getComponentType();
	}

	@Override
	public Object read(int index) {
		try {
			return readHandle.invoke(index);
		} catch (Throwable ex) {
			throw new AssertionError(ex);
		}
	}

	@Override
	public void write(int index, Object data) {
		try {
			writeHandle.invoke(index, data);
		} catch (Throwable ex) {
			throw new AssertionError(ex);
		}
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

	private int index(int physicalIndex) {
		//assumes (physicalIndex + head) >= 0
		//I'd assert but that would add bytes to the method, hampering inlining.
		return (physicalIndex + head) % capacity;
	}

	public static StorageFactory factory() {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				return new CircularArrayConcreteStorage(storage);
			}
		};
	}
}
