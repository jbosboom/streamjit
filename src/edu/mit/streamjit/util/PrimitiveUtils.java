package edu.mit.streamjit.util;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.ImmutableMap;

/**
 * Provides utility methods on primitives not provided in wrapper classes, Guava
 * wrapper or primitive utilities, or
 * {@link com.google.common.primitives.Primitives}.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/1/2014
 */
public final class PrimitiveUtils {
	private PrimitiveUtils() {}

	private static final ImmutableMap<Class<?>, Integer> SIZEOF_MAP = ImmutableMap.<Class<?>, Integer>builder()
			.put(void.class, 0)
			.put(boolean.class, 1)
			.put(byte.class, 1)
			.put(char.class, 2)
			.put(short.class, 2)
			.put(int.class, 4)
			.put(long.class, 8)
			.put(float.class, 4)
			.put(double.class, 8)
			.build();

	/**
	 * Returns the size of the given primitive type, in bytes.  void is
	 * considered to have size 0.
	 * @param prim a primitive type
	 * @return the size of the given primitive type, in bytes
	 */
	public static int sizeof(Class<?> prim) {
		checkArgument(prim.isPrimitive(), "not a primitive: %s", prim);
		return SIZEOF_MAP.get(prim);
	}
}
