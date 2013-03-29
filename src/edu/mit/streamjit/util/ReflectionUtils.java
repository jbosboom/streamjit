package edu.mit.streamjit.util;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Array;
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

	/**
	 * Returns the Class object representing an array of the type represented
	 * by the given Class object.  That is, this method is the inverse of
	 * Class.getComponentType().
	 * @param <T> the type of the given Class object
	 * @param klass the type to get an array type for
	 * @return the type of an array of the given type
	 */
	@SuppressWarnings("unchecked")
	public static <T> Class<T[]> getArrayType(Class<T> klass) {
		return (Class<T[]>)Array.newInstance(klass, 0).getClass();
	}

	/**
	 * Returns true if the given class uses Object equality (a.k.a. identity
	 * equality) and false otherwise.  Specifically, this method returns true if
	 * the given class inherits its equals() or hashCode() implementation from
	 * Object.  (These methods should be overridden together, but they aren't
	 * always; this method assumes the caller will use both.)
	 *
	 * If the given Class object represents a primitive type, this method will
	 * return false, as primitive types use value-based equality.  (Even the
	 * pseudo-type void vacuously uses value-based equality, as there are no
	 * values of void type.)
	 *
	 * Note that a false return does not mean equals() and hashCode() are
	 * implemented correctly -- indeed, an equals() implementation of
	 * "super.equals(o)" with a corresponding hashCode() implementation will
	 * return false.
	 * @param klass the class to test
	 * @return true if the given class uses Object equality
	 */
	public static boolean usesObjectEquality(Class<? extends Object> klass) {
		if (klass.isPrimitive())
			return false;
		try {
			return klass.getMethod("equals", new Class<?>[]{Object.class}).getDeclaringClass().equals(Object.class)
					|| klass.getMethod("hashCode").getDeclaringClass().equals(Object.class);
		} catch (NoSuchMethodException ex) {
			//Every class has an equals() and hashCode().
			throw new AssertionError("Can't happen! No equals() or hashCode()?", ex);
		}
	}
}
