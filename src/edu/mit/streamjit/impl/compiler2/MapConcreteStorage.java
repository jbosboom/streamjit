package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.util.Combinators;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.Map;

/**
 * An unsynchronized ConcreteStorage implementation using a Map<Integer, T>.
 * As its adjust is a no-op, only useful as internal or initialization storage.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/27/2013
 */
public final class MapConcreteStorage implements ConcreteStorage {
	private static final MethodHandle MAP_GET, MAP_PUT;
	static {
		try {
			MAP_GET = MethodHandles.publicLookup().findVirtual(Map.class, "get", MethodType.methodType(Object.class, Object.class))
					.asType(MethodType.methodType(Object.class, Map.class, Integer.class));
			MAP_PUT = MethodHandles.publicLookup().findVirtual(Map.class, "put", MethodType.methodType(boolean.class, Object.class, Object.class))
					.asType(MethodType.methodType(void.class, Map.class, Integer.class, Object.class));
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError("Can't happen! No Map.get?", ex);
		}
	}

	private final Class<?> type;
	private final Map<Integer, Object> map = new HashMap<>();
	private final MethodHandle readHandle, writeHandle;
	public MapConcreteStorage(Class<?> type) {
		this.type = type;
		this.readHandle = MAP_GET.bindTo(map).asType(MethodType.methodType(type, Integer.class));
		this.writeHandle = MAP_PUT.bindTo(map).asType(MethodType.methodType(void.class, Integer.class, type));
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
		return Combinators.nop();
	}

	@Override
	public String toString() {
		return map.toString();
	}
}
