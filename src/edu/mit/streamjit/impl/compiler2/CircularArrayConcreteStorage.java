package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.impl.compiler2.ConcreteStorage;
import edu.mit.streamjit.util.Combinators;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;

/**
 * A ConcreteStorage backed by a circular array.  The live elements after a
 * buffer-adjust are at indices [readHead, writeHead) (possibly wrapped around
 * the end of the array, of course).
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/10/2013
 */
public class CircularArrayConcreteStorage implements ConcreteStorage {
	private static final Lookup LOOKUP = MethodHandles.lookup();
	private final Object array;
	private final int capacity, throughput;
	private int readHead, writeHead;
	public CircularArrayConcreteStorage(Class<?> type, int capacity, int throughput, Object initialDataArray) {
		this.array = Array.newInstance(type, capacity);
		this.capacity = capacity;
		this.throughput = throughput;
		this.readHead = 0;
		this.writeHead = Array.getLength(initialDataArray);
		System.arraycopy(array, 0, initialDataArray, 0, Array.getLength(initialDataArray));
	}

	@Override
	public Class<?> type() {
		return array.getClass().getComponentType();
	}

	@Override
	public MethodHandle readHandle() {
		try {
			MethodHandle readIndex = LOOKUP.findVirtual(CircularArrayConcreteStorage.class, "readIndex", MethodType.methodType(int.class, int.class));
			MethodHandle arrayGetter = MethodHandles.arrayElementGetter(array.getClass()).bindTo(array);
			return MethodHandles.filterReturnValue(readIndex, arrayGetter);
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError("can't happen", ex);
		}
	}

	@Override
	public MethodHandle writeHandle() {
		try {
			MethodHandle writeIndex = LOOKUP.findVirtual(CircularArrayConcreteStorage.class, "writeIndex", MethodType.methodType(int.class, int.class));
			MethodHandle arraySetter = MethodHandles.arrayElementSetter(array.getClass()).bindTo(array);
			return MethodHandles.filterArguments(arraySetter, 0, writeIndex);
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError("can't happen", ex);
		}
	}

	@Override
	public MethodHandle adjustHandle() {
		try {
			return LOOKUP.bind(this, "adjust", MethodType.methodType(void.class));
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError("can't happen", ex);
		}
	}

	private int readIndex(int physicalIndex) {
		return (physicalIndex + readHead) % capacity;
	}

	private int writeIndex(int physicalIndex) {
		return (physicalIndex + writeHead) % capacity;
	}

	private void adjust() {
		readHead = (readHead + throughput) % capacity;
		writeHead = (writeHead + throughput) % capacity;
	}
}
