package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableSortedSet;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A ConcreteStorage implementation using a Map<Integer, T>.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/27/2013
 */
public final class MapConcreteStorage implements ConcreteStorage {
	private static final MethodHandle MAP_GET, MAP_PUT, ADJUST;
	static {
		try {
			MAP_GET = MethodHandles.publicLookup().findVirtual(Map.class, "get", MethodType.methodType(Object.class, Object.class))
					.asType(MethodType.methodType(Object.class, Map.class, int.class));
			MAP_PUT = MethodHandles.publicLookup().findVirtual(Map.class, "put", MethodType.methodType(Object.class, Object.class, Object.class))
					.asType(MethodType.methodType(void.class, Map.class, int.class, Object.class));
			ADJUST = MethodHandles.lookup().findVirtual(MapConcreteStorage.class, "adjust", MethodType.methodType(void.class));
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError("Can't happen! No Map.get?", ex);
		}
	}

	private final Class<?> type;
	private final Map<Integer, Object> map = new ConcurrentHashMap<>();
	private final MethodHandle readHandle, writeHandle, adjustHandle;
	private final int minReadIndex, throughput;
	public MapConcreteStorage(Storage s) {
		this.type = s.type();
		this.readHandle = MAP_GET.bindTo(map).asType(MethodType.methodType(type, int.class));
		this.writeHandle = MAP_PUT.bindTo(map).asType(MethodType.methodType(void.class, int.class, type));
		this.adjustHandle = ADJUST.bindTo(this);
		this.minReadIndex = s.readIndices().first();
		this.throughput = s.throughput();
	}

	@Override
	public Class<?> type() {
		return type;
	}

	@Override
	public Object read(int index) {
		try {
			return readHandle().invoke(index);
		} catch (Throwable ex) {
			throw new AssertionError(ex);
		}
	}

	@Override
	public void write(int index, Object data) {
		try {
			writeHandle().invoke(index, data);
		} catch (Throwable ex) {
			throw new AssertionError(ex);
		}
	}

	@Override
	public void adjust() {
		//This is pretty slow, but we need them in order so we don't overwrite.
		ImmutableSortedSet<Integer> indices = ImmutableSortedSet.copyOf(map.keySet());
		for (int i : indices) {
			int newReadIndex = i - throughput;
			Object item = map.remove(i);
			if (newReadIndex >= minReadIndex) {
				Object overwrote = map.put(newReadIndex, item);
				assert overwrote == null : newReadIndex;
			}
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
		return writeHandle;
	}

	@Override
	public MethodHandle adjustHandle() {
		return adjustHandle;
	}

	@Override
	public String toString() {
		return map.toString();
	}

	public static StorageFactory factory() {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				return new MapConcreteStorage(storage);
			}
		};
	}
}
