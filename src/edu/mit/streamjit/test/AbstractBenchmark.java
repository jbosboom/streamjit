package edu.mit.streamjit.test;

import edu.mit.streamjit.util.ConstructorSupplier;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import edu.mit.streamjit.api.OneToOneElement;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/13/2013
 */
public abstract class AbstractBenchmark implements Benchmark {
	private final String name;
	private final Supplier<? extends OneToOneElement> supplier;
	private final ImmutableList<Dataset> inputs;
	public AbstractBenchmark(String name, Supplier<? extends OneToOneElement> supplier, Dataset firstInput, Dataset... moreInputs) {
		this.name = name;
		this.supplier = supplier;
		this.inputs = ImmutableList.copyOf(Lists.asList(firstInput, moreInputs));
	}
	public <T> AbstractBenchmark(String name, Class<? extends OneToOneElement> streamClass, Iterable<?> arguments, Dataset firstInput, Dataset... moreInputs) {
		this(name, new ConstructorSupplier<>(streamClass, arguments), firstInput, moreInputs);
	}
	public AbstractBenchmark(String name, Class<? extends OneToOneElement> streamClass, Dataset firstInput, Dataset... moreInputs) {
		this(name, streamClass, ImmutableList.of(), firstInput, moreInputs);
	}

	@Override
	@SuppressWarnings("unchecked")
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
}
