package edu.mit.streamjit.test;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.util.ReflectionUtils;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/13/2013
 */
public abstract class AbstractBenchmark implements Benchmark {
	private final String name;
	private final Supplier<? extends OneToOneElement<Object, Object>> supplier;
	private final ImmutableList<Dataset> inputs;
	public AbstractBenchmark(String name, Supplier<? extends OneToOneElement<Object, Object>> supplier, Dataset firstInput, Dataset... moreInputs) {
		this.name = name;
		this.supplier = supplier;
		this.inputs = ImmutableList.copyOf(Lists.asList(firstInput, moreInputs));
	}
	public AbstractBenchmark(String name, Class<?> streamClass, Iterable<?> arguments, Dataset firstInput, Dataset... moreInputs) {
		this(name, new ConstructorSupplier(streamClass, arguments), firstInput, moreInputs);
	}
	public AbstractBenchmark(String name, Class<?> streamClass, Dataset firstInput, Dataset... moreInputs) {
		this(name, streamClass, ImmutableList.of(), firstInput, moreInputs);
	}

	@Override
	public final OneToOneElement<Object, Object> instantiate() {
		return supplier.get();
	}
	@Override
	public final List<Dataset> inputs() {
		return inputs;
	}
	@Override
	public final String toString() {
		return name;
	}

	private static class ConstructorSupplier implements Supplier<OneToOneElement<Object, Object>> {
		private final Constructor<?> ctor;
		private final ImmutableList<?> arguments;
		private ConstructorSupplier(Class<?> klass, Iterable<?> arguments) {
			this.arguments = ImmutableList.copyOf(arguments);
			try {
				this.ctor = ReflectionUtils.findConstructor(klass, this.arguments);
			} catch (NoSuchMethodException ex) {
				throw new UndeclaredThrowableException(ex);
			}
		}
		@Override
		@SuppressWarnings("unchecked")
		public OneToOneElement<Object, Object> get() {
			try {
				return (OneToOneElement<Object, Object>)ctor.newInstance(arguments.toArray());
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
				throw new UndeclaredThrowableException(ex);
			}
		}
	}
}
