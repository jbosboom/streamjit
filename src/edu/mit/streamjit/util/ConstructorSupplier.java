package edu.mit.streamjit.util;

import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.Invokable;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Creates T instances by calling a constructor with the given set of arguments.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/20/2013
 */
public class ConstructorSupplier<T> implements Supplier<T> {
	private final Invokable<T, T> ctor;
	private final ImmutableList<?> arguments;
	public ConstructorSupplier(TypeToken<T> type, Iterable<?> arguments) {
		this.arguments = ImmutableList.copyOf(arguments);
		try {
			this.ctor = type.constructor(ReflectionUtils.findConstructor(type.getRawType(), this.arguments));
		} catch (NoSuchMethodException ex) {
			throw new UndeclaredThrowableException(ex);
		}
	}
	public ConstructorSupplier(TypeToken<T> type, Object... arguments) {
		this(type, Arrays.asList(arguments));
	}
	public ConstructorSupplier(Class<T> klass, Iterable<?> arguments) {
		this(TypeToken.of(klass), arguments);
	}
	public ConstructorSupplier(Class<T> klass, Object... arguments) {
		this(klass, Arrays.asList(arguments));
	}
	protected ConstructorSupplier(Iterable<?> arguments) {
		TypeToken<T> type = new TypeToken<T>(getClass()) {};
		this.arguments = ImmutableList.copyOf(arguments);
		try {
			this.ctor = type.constructor(ReflectionUtils.findConstructor(type.getRawType(), this.arguments));
		} catch (NoSuchMethodException ex) {
			throw new UndeclaredThrowableException(ex);
		}
	}
	protected ConstructorSupplier(Object... arguments) {
		this(Arrays.asList(arguments));
	}

	@Override
	public T get() {
		try {
			return ctor.invoke(null, arguments.toArray());
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
			throw new UndeclaredThrowableException(ex);
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final ConstructorSupplier<?> other = (ConstructorSupplier<?>)obj;
		if (!Objects.equals(this.ctor, other.ctor))
			return false;
		if (!Objects.equals(this.arguments, other.arguments))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 71 * hash + Objects.hashCode(this.ctor);
		hash = 71 * hash + Objects.hashCode(this.arguments);
		return hash;
	}

	private static final Joiner ARG_JOINER = Joiner.on(", ");
	@Override
	public String toString() {
		return "new " + ctor.getDeclaringClass().getCanonicalName() + "(" + ARG_JOINER.join(arguments)+")";
	}
}
