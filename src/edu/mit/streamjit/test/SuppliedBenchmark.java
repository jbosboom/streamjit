package edu.mit.streamjit.test;

import edu.mit.streamjit.util.ConstructorSupplier;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.api.OneToOneElement;

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
public class SuppliedBenchmark extends AbstractBenchmark {
	private final Supplier<? extends OneToOneElement> supplier;
	public SuppliedBenchmark(String name, Supplier<? extends OneToOneElement> supplier, Dataset firstInput, Dataset... moreInputs) {
		super(name, firstInput, moreInputs);
		this.supplier = supplier;
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
}
