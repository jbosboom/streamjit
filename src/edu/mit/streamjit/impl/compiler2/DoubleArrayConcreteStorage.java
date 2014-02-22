package edu.mit.streamjit.impl.compiler2;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.util.Combinators;
import static edu.mit.streamjit.util.LookupUtils.findGetter;
import static edu.mit.streamjit.util.LookupUtils.findStatic;
import static edu.mit.streamjit.util.LookupUtils.findVirtual;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;
import java.util.Map;

/**
 * A ConcreteStorage backed by double-buffered arrays.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/10/2013
 */
public class DoubleArrayConcreteStorage implements ConcreteStorage {//, BulkReadableConcreteStorage, BulkWritableConcreteStorage {
	private static final Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle ADJUST = findVirtual(LOOKUP, DoubleArrayConcreteStorage.class, "adjust", void.class);;
	private static final MethodHandle READ_ARRAY_GETTER = findGetter(LOOKUP, DoubleArrayConcreteStorage.class, "readArray", Object.class);
	private static final MethodHandle WRITE_ARRAY_GETTER = findGetter(LOOKUP, DoubleArrayConcreteStorage.class, "writeArray", Object.class);
	private Object readArray, writeArray;
	private final int capacity, throughput, readOffset;
	private final MethodHandle readHandle, writeHandle, adjustHandle;
	public DoubleArrayConcreteStorage(Storage s) {
		this.capacity = s.steadyStateCapacity();
		assert capacity > 0 : s + " has capacity "+capacity;
		this.throughput = s.throughput();
		assert capacity == 2*throughput : "can't double buffer "+s;
		this.readArray = Array.newInstance(s.type(), throughput);
		this.writeArray = Array.newInstance(s.type(), throughput);

		ImmutableSet<ActorGroup> relevantGroups = ImmutableSet.<ActorGroup>builder().addAll(s.upstreamGroups()).addAll(s.downstreamGroups()).build();
		Map<ActorGroup, Integer> oneMap = Maps.asMap(relevantGroups, new Function<ActorGroup, Integer>() {
			@Override
			public Integer apply(ActorGroup input) {
				return 1;
			}
		});
		this.readOffset = s.readIndices(oneMap).first();
		int writeOffset = s.writeIndices(oneMap).first();

		MethodHandle arrayGetter = MethodHandles.arrayElementGetter(readArray.getClass());
		MethodHandle arraySetter = MethodHandles.arrayElementSetter(writeArray.getClass());
		MethodHandle readArrayGetter = READ_ARRAY_GETTER.bindTo(this).asType(MethodType.methodType(readArray.getClass()));
		MethodHandle writeArrayGetter = WRITE_ARRAY_GETTER.bindTo(this).asType(MethodType.methodType(writeArray.getClass()));
		this.readHandle = MethodHandles.filterArguments(MethodHandles.foldArguments(arrayGetter, readArrayGetter),
				0, Combinators.sub(MethodHandles.identity(int.class), readOffset));
		this.writeHandle = MethodHandles.filterArguments(MethodHandles.foldArguments(arraySetter, writeArrayGetter),
				0, Combinators.sub(MethodHandles.identity(int.class), writeOffset));
		this.adjustHandle = ADJUST.bindTo(this);
	}

	@Override
	public Class<?> type() {
		return readArray.getClass().getComponentType();
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
//		System.out.println(String.format("write(%d, %s)", index, data));
		try {
			//Pretend the read and write arrays are contiguous.
			index -= readOffset;
			if (index < throughput)
				Array.set(readArray, index, data);
			else
				Array.set(writeArray, index-throughput, data);
		} catch (Throwable ex) {
			throw new AssertionError(String.format("write(%d, %s)", index, data), ex);
		}
	}

	@Override
	public void adjust() {
		Object t = readArray;
		readArray = writeArray;
		writeArray = t;
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

//	@Override
//	public int bulkRead(Buffer dest, int index, int count) {
//		assert type().equals(Object.class);
//		index = index(capacity, head, index);
//		int countBeforeEnd = Math.min(count, capacity - index);
//		int written = dest.write((Object[])array, index, countBeforeEnd);
//		if (written != countBeforeEnd) //short write
//			return written;
//
//		int remaining = count - countBeforeEnd;
//		written += dest.write((Object[])array, 0, remaining);
//		return written; //short write or not, we're done
//	}
//
//	@Override
//	public void bulkWrite(Buffer source, int index, int count) {
//		assert type().equals(Object.class);
//		index = index(capacity, head, index);
//		int countBeforeEnd = Math.min(count, capacity - index);
//		int itemsRead = source.read((Object[])array, index, countBeforeEnd);
//		assert itemsRead == countBeforeEnd;
//		int remaining = count - itemsRead;
//		int secondItemsRead = source.read((Object[])array, 0, remaining);
//		assert secondItemsRead == remaining;
//		assert itemsRead + secondItemsRead == count;
//	}

	public static StorageFactory factory() {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				if (storage.steadyStateCapacity() == 0)
					return new EmptyConcreteStorage(storage);
				return new DoubleArrayConcreteStorage(storage);
			}
		};
	}
}
