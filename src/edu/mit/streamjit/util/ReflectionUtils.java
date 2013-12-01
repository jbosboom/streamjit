package edu.mit.streamjit.util;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

	/**
	 * Returns true if the caller's caller is a method in the given class, and
	 * throws AssertionError otherwise.  This method is useful for assertion
	 * statements, but should not be used to make security decisions.
	 *
	 * This method returns a value so that it can be used in assertions (thus
	 * disabling the expensive stack check when assertions are disabled).  If
	 * this method returns, it returns true.
	 * @param expectedCaller the expected class of the caller's caller
	 * @return true
	 * @throws AssertionError if the caller's caller is unexpected
	 */
	public static boolean calledDirectlyFrom(Class<?> expectedCaller) {
		StackTraceElement[] trace = Thread.currentThread().getStackTrace();
		//trace[0]: Thread.getStackTrace
		//trace[1]: ReflectionUtils.calledDirectlyFrom
		//trace[2]: caller
		//trace[3]: caller's caller
		StackTraceElement callerCaller = trace[3];
		if (!callerCaller.getClassName().equals(expectedCaller.getName()))
			//This exception carries a stack trace, so we don't need to store
			//it in the message.
			throw new AssertionError(String.format(
					"Expected caller %s, but was %s",
					expectedCaller, callerCaller));
		return true;
	}

	/**
	 * Returns the unique applicable constructor for the given class and
	 * arguments.
	 *
	 * TODO: what if the constructor throws some other exception?
	 * @param <T> the type of the class to find a constructor for
	 * @param klass the class to find a constructor for
	 * @param arguments the arguments
	 * @return the unique applicable constructor for the given class and
	 * arguments
	 * @throws NoSuchMethodException if no constructor is applicable, or more
	 * than one constructor is applicable
	 */
	public static <T> Constructor<T> findConstructor(Class<T> klass, List<?> arguments) throws NoSuchMethodException {
		//This is safe because we never assign into the constructors array.  See
		//the comments on Class.getConstructors().
		@SuppressWarnings("unchecked")
		Constructor<T>[] constructors = (Constructor<T>[])klass.getConstructors();
		List<Constructor<T>> retvals = new ArrayList<>();
		Map<Constructor<T>, Throwable> exceptions = new HashMap<>();
		for (Constructor<T> ctor : constructors)
			try {
				ctor.setAccessible(true);
				ctor.newInstance(arguments.toArray());
				retvals.add(ctor);
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
				exceptions.put(ctor, ex);
			}
		if (retvals.isEmpty())
			throw new NoSuchMethodException("Couldn't create a "+klass+" from "+arguments+": exceptions "+exceptions);
		if (retvals.size() > 1)
			throw new NoSuchMethodException("Creating a "+klass+" from "+arguments+" was ambiguous: "+retvals);
		return retvals.get(0);
	}

	public static <T> ImmutableSet<Class<?>> getAllSupertypes(Class<T> klass) {
		ImmutableSet.Builder<Class<?>> builder = ImmutableSet.builder();
		builder.add(klass);
		for (Class<?> c : klass.getInterfaces())
			builder.add(c);
		if (klass.getSuperclass() != null)
			builder.addAll(getAllSupertypes(klass.getSuperclass()));
		return builder.build();
	}

	public static boolean containsVariableOrWildcard(Type type) {
		if (type instanceof TypeVariable || type instanceof WildcardType)
			return true;
		if (type instanceof ParameterizedType) {
			for (Type t : ((ParameterizedType)type).getActualTypeArguments())
				if (containsVariableOrWildcard(t))
					return true;
			//TODO: should we check the owner type or not?
			return false;
		}
		if (type instanceof GenericArrayType)
			return containsVariableOrWildcard(((GenericArrayType)type).getGenericComponentType());
		return false;
	}
}
