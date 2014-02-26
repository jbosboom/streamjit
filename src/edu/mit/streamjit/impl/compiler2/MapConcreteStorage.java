package edu.mit.streamjit.impl.compiler2;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import edu.mit.streamjit.util.Combinators;
import static edu.mit.streamjit.util.LookupUtils.findVirtual;
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
	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle MAP_GET = findVirtual(LOOKUP, Map.class, "get", Object.class, Object.class)
			.asType(MethodType.methodType(Object.class, Map.class, int.class));
	private static final MethodHandle MAP_PUT = findVirtual(LOOKUP, Map.class, "put", Object.class, Object.class, Object.class)
			.asType(MethodType.methodType(void.class, Map.class, int.class, Object.class));
	private static final MethodHandle ADJUST = findVirtual(LOOKUP, MapConcreteStorage.class, "adjust", void.class);
	private final Class<?> type;
	private final Map<Integer, Object> map = new ConcurrentHashMap<>();
	private final MethodHandle readHandle, writeHandle, adjustHandle;
	private final int minReadIndex, throughput;
	private MapConcreteStorage(Class<?> type, MethodHandle adjustHandle, int minReadIndex, int throughput) {
		this.type = type;
		this.readHandle = MAP_GET.bindTo(map).asType(MethodType.methodType(type, int.class));
		this.writeHandle = MAP_PUT.bindTo(map).asType(MethodType.methodType(void.class, int.class, type));
		this.adjustHandle = adjustHandle.bindTo(this);
		this.minReadIndex = minReadIndex;
		this.throughput = throughput;
	}
	public static MapConcreteStorage create(Storage s) {
		ImmutableSet<ActorGroup> relevantGroups = ImmutableSet.<ActorGroup>builder().addAll(s.upstreamGroups()).addAll(s.downstreamGroups()).build();
		return new MapConcreteStorage(s.type(), ADJUST, s.readIndices(Maps.asMap(relevantGroups, new Function<ActorGroup, Integer>() {
			@Override
			public Integer apply(ActorGroup input) {
				return 1;
			}
		})).first(), s.throughput());
	}
	public static MapConcreteStorage createNopAdjust(Storage s) {
		return new MapConcreteStorage(s.type(), Combinators.nop(Object.class), Integer.MAX_VALUE, Integer.MAX_VALUE);
	}
	//TODO: create(Storage s, Map<ActorGroup, Integer> schedule) if we want adjusting

	@Override
	public Class<?> type() {
		return type;
	}

	@Override
	public Object read(int index) {
		try {
			return readHandle().invoke(index);
		} catch (Throwable ex) {
			throw new AssertionError(String.format("%s.read(%d, %s)", this, index), ex);
		}
	}

	@Override
	public void write(int index, Object data) {
		try {
			writeHandle().invoke(index, data);
		} catch (Throwable ex) {
			throw new AssertionError(String.format("%s.write(%d, %s)", this, index, data), ex);
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
				return create(storage);
			}
		};
	}

	public static StorageFactory initFactory() {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				return createNopAdjust(storage);
			}
		};
	}
}
