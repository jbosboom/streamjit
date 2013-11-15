package edu.mit.streamjit.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;

/**
 * Convenience methods for MethodHandle.Lookup methods that must succeed.  This
 * allows initializing static variables without requiring a static initializer
 * block to catch the exceptions that will never be thrown anyway.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/14/2013
 */
public final class LookupUtils {
	private LookupUtils() {}

	public static MethodHandle findVirtual(Lookup lookup, Class<?> container, String name, MethodType type) {
		try {
			return lookup.findVirtual(container, name, type);
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError(ex);
		}
	}

	public static MethodHandle findVirtual(Lookup lookup, Class<?> container, String name, Class<?> returnType, Class<?>... parameterTypes) {
		try {
			return lookup.findVirtual(container, name, MethodType.methodType(returnType, parameterTypes));
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError(ex);
		}
	}

	public static MethodHandle findStatic(Lookup lookup, Class<?> container, String name, MethodType type) {
		try {
			return lookup.findStatic(container, name, type);
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError(ex);
		}
	}

	public static MethodHandle findStatic(Lookup lookup, Class<?> container, String name, Class<?> returnType, Class<?>... parameterTypes) {
		try {
			return lookup.findStatic(container, name, MethodType.methodType(returnType, parameterTypes));
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError(ex);
		}
	}

	public static MethodHandle findConstructor(Lookup lookup, Class<?> container, Class<?>... parameterTypes) {
		try {
			return lookup.findConstructor(container, MethodType.methodType(void.class, parameterTypes));
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError(ex);
		}
	}

	public static MethodHandle findGetter(Lookup lookup, Class<?> container, String name, Class<?> type) {
		try {
			return lookup.findGetter(container, name, type);
		} catch (NoSuchFieldException | IllegalAccessException ex) {
			throw new AssertionError(ex);
		}
	}

	public static MethodHandle findSetter(Lookup lookup, Class<?> container, String name, Class<?> type) {
		try {
			return lookup.findSetter(container, name, type);
		} catch (NoSuchFieldException | IllegalAccessException ex) {
			throw new AssertionError(ex);
		}
	}
}
