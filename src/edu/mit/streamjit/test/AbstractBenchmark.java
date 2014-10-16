package edu.mit.streamjit.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;

/**
 * A skeletal Benchmark implementation that manages the name and inputs but
 * leaves instantiate() abstract.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/27/2013
 */
public abstract class AbstractBenchmark implements Benchmark {
	private final String name;
	private final ImmutableList<Dataset> inputs;
	public AbstractBenchmark(String name, Dataset firstInput, Dataset... moreInputs) {
		this.name = name;
		this.inputs = ImmutableList.copyOf(Lists.asList(firstInput, moreInputs));
	}
	public AbstractBenchmark(Dataset firstInput, Dataset... moreInputs) {
		if (!getClass().getSimpleName().isEmpty())
			this.name = getClass().getSimpleName();
		else {
			String binaryName = getClass().getName();
			this.name = binaryName.substring(binaryName.lastIndexOf('.')+1, binaryName.length()-1);
		}
		this.inputs = ImmutableList.copyOf(Lists.asList(firstInput, moreInputs));
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
