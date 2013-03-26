package edu.mit.streamjit.util;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * Contains reflection utilities.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/26/2013
 */
public final class ReflectionUtils {
	private ReflectionUtils() {}

	/**
	 * Returns an immutable set of all fields in the given class or its
	 * superclasses and superinterfaces, with any access modifier, static or
	 * nonstatic.
	 * @param klass the class to get fields of
	 * @return an immutable set of all fields in the class, including inherited
	 * and static fields
	 */
	public static ImmutableSet<Field> getAllFields(Class<?> klass) {
		checkNotNull(klass);
		ImmutableSet.Builder<Field> builder = ImmutableSet.builder();
		while (klass != null) {
			builder.addAll(Arrays.asList(klass.getDeclaredFields()));
			for (Class<?> i : klass.getInterfaces())
				builder.addAll(Arrays.asList(i.getDeclaredFields()));
			klass = klass.getSuperclass();
		}
		return builder.build();
	}
}
