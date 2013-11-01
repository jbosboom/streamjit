package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableSortedSet;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.Map;

/**
 * A synchronized ConcreteStorage implementation using two Map<Integer, T>s.
 * Unlikely to provide good performance.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/31/2013
 */
public class DoubleMapConcreteStorage implements ConcreteStorage {
	private static final MethodHandle MAP_GET, MAP_PUT, ADJUST;
	static {
		try {
			MAP_GET = MethodHandles.publicLookup().findVirtual(Map.class, "get", MethodType.methodType(Object.class, Object.class))
					.asType(MethodType.methodType(Object.class, Map.class, Integer.class));
			MAP_PUT = MethodHandles.publicLookup().findVirtual(Map.class, "put", MethodType.methodType(boolean.class, Object.class, Object.class))
					.asType(MethodType.methodType(void.class, Map.class, Integer.class, Object.class));
			ADJUST = MethodHandles.lookup().findVirtual(DoubleMapConcreteStorage.class, "adjust", MethodType.methodType(void.class));
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError("Can't happen! No Map.get?", ex);
		}
	}

	private final Class<?> type;
	private final Map<Integer, Object> reader = new HashMap<>(), writer = new HashMap<>();
	private final MethodHandle readHandle, writeHandle, adjustHandle;
	/**
	 * The smallest index that can be read.  Entries shifted beyond this index
	 * need not be retained.
	 */
	private final int minReadIndex;
	private final int writeStartIndex;
	private final int throughput;
	public DoubleMapConcreteStorage(Storage s) {
		this.type = s.type();
		this.minReadIndex = s.readIndices().first();
		this.writeStartIndex = s.readIndices().last()+1;
		this.throughput = s.throughput();
		this.readHandle = MAP_GET.bindTo(reader).asType(MethodType.methodType(type, Integer.class));
		this.writeHandle = MAP_PUT.bindTo(writer).asType(MethodType.methodType(void.class, Integer.class, type));
		this.adjustHandle = ADJUST.bindTo(this);
	}

	@Override
	public Class<?> type() {
		return type;
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

	private void adjust() {
		ImmutableSortedSet<Integer> oldReadIndices = ImmutableSortedSet.copyOf(reader.keySet());
		for (int i : oldReadIndices) {
			int newReadIndex = i - throughput;
			Object item = reader.remove(i);
			if (newReadIndex >= minReadIndex)
				reader.put(newReadIndex, item);
		}
		for (Map.Entry<Integer, Object> e : writer.entrySet()) {
			Object old = reader.put(e.getKey()+writeStartIndex, e.getValue());
			assert old == null : "overwrote?";
		}
		writer.clear();
	}

	public static StorageFactory factory() {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				return new DoubleMapConcreteStorage(storage);
			}
		};
	}
}
