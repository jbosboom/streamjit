package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.util.Combinators;
import static edu.mit.streamjit.util.LookupUtils.findGetter;
import static edu.mit.streamjit.util.LookupUtils.findStatic;
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
public class CircularArrayConcreteStorage implements ConcreteStorage, BulkReadableConcreteStorage, BulkWritableConcreteStorage {
	private static final Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle INDEX = findStatic(LOOKUP, CircularArrayConcreteStorage.class, "index", int.class, int.class, int.class, int.class);
	private static final MethodHandle ADJUST = findVirtual(LOOKUP, CircularArrayConcreteStorage.class, "adjust", void.class);;
	private static final MethodHandle HEAD_GETTER = findGetter(LOOKUP, CircularArrayConcreteStorage.class, "head", int.class);
	private final Object array;
	private final int capacity, throughput;
	private int head;
	private final MethodHandle readHandle, writeHandle, adjustHandle;
	public CircularArrayConcreteStorage(Storage s) {
		this.capacity = s.steadyStateCapacity();
		assert capacity > 0 : s + " has capacity "+capacity;
		this.array = Array.newInstance(s.type(), capacity);
		this.throughput = s.throughput();
		this.head = 0;

		MethodHandle arrayLength = Combinators.arraylength(array.getClass()).bindTo(array);
		MethodHandle index = MethodHandles.foldArguments(INDEX, arrayLength);
		index = MethodHandles.foldArguments(index, HEAD_GETTER.bindTo(this));
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

	@Override
	public void bulkRead(Buffer dest, int index, int count) {
		assert type().equals(Object.class);
		index = index(capacity, head, index);
		int countBeforeEnd = Math.min(count, capacity - index);
		for (int written = 0; written < countBeforeEnd;)
			written += dest.write((Object[])array, index + written, countBeforeEnd - written);
		int remaining = count - countBeforeEnd;
		for (int written = 0; written < remaining;)
			written += dest.write((Object[])array, written, remaining - written);
	}

	@Override
	public void bulkWrite(Buffer source, int index, int count) {
		assert type().equals(Object.class);
		index = index(capacity, head, index);
		int countBeforeEnd = Math.min(count, capacity - index);
		int itemsRead = source.read((Object[])array, index, countBeforeEnd);
		assert itemsRead == countBeforeEnd;
		int remaining = count - itemsRead;
		int secondItemsRead = source.read((Object[])array, 0, remaining);
		assert secondItemsRead == remaining;
		assert itemsRead + secondItemsRead == count;
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
				return new CircularArrayConcreteStorage(storage);
			}
		};
	}
}
