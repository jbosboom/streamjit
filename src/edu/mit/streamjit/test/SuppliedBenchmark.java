package edu.mit.streamjit.test;

import edu.mit.streamjit.util.ConstructorSupplier;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import edu.mit.streamjit.api.OneToOneElement;
import java.util.List;

/**
 * A Benchmark implementation that instantiates a stream graph from a Supplier
 * instance. Also includes convenience constructors for common Supplier
 * implementations, such as constructors.
 * <p/>
 * This class is nonfinal to allow subclasses to specify constructor arguments.
 * The subclasses then have a no-arg constructor required for the ServiceLoader
 * mechanism. The Benchmark interface methods are final.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/13/2013
 */
public class SuppliedBenchmark implements Benchmark {
	private final String name;
	private final Supplier<? extends OneToOneElement> supplier;
	private final ImmutableList<Dataset> inputs;
	public SuppliedBenchmark(String name, Supplier<? extends OneToOneElement> supplier, Dataset firstInput, Dataset... moreInputs) {
		this.name = name;
		this.supplier = supplier;
		this.inputs = ImmutableList.copyOf(Lists.asList(firstInput, moreInputs));
	}
	public <T> SuppliedBenchmark(String name, Class<? extends OneToOneElement> streamClass, Iterable<?> arguments, Dataset firstInput, Dataset... moreInputs) {
		this(name, new ConstructorSupplier<>(streamClass, arguments), firstInput, moreInputs);
	}
	public SuppliedBenchmark(String name, Class<? extends OneToOneElement> streamClass, Dataset firstInput, Dataset... moreInputs) {
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
