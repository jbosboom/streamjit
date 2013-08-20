package edu.mit.streamjit.util;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;

/**
 * Creates T instances by calling a constructor with the given set of arguments.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/20/2013
 */
public final class ConstructorSupplier<T> implements Supplier<T> {
	private final Constructor<T> ctor;
	private final ImmutableList<?> arguments;
	public ConstructorSupplier(Class<T> klass, Iterable<?> arguments) {
		this.arguments = ImmutableList.copyOf(arguments);
		try {
			this.ctor = ReflectionUtils.findConstructor(klass, this.arguments);
		} catch (NoSuchMethodException ex) {
			throw new UndeclaredThrowableException(ex);
		}
	}
	public ConstructorSupplier(Class<T> klass, Object... arguments) {
		this(klass, Arrays.asList(arguments));
	}

	@Override
    @SuppressWarnings(value = "unchecked")
	public T get() {
		try {
			return ctor.newInstance(arguments.toArray());
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
			throw new UndeclaredThrowableException(ex);
		}
	}
}
