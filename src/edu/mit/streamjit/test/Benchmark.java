package edu.mit.streamjit.test;

import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.impl.blob.Buffer;
import java.util.List;

/**
 * A benchmark stream graph.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/12/2013
 */
public interface Benchmark {
	/**
	 * Instantiates the benchmark stream graph.  Always returns a new instance.
	 *
	 * Note that the actual graph is often not an Object->Object graph, but
	 * generics suck, so we'll opt-out here.
	 * @return a new instance of the benchmark stream graph
	 */
	public OneToOneElement<Object, Object> instantiate();

	/**
	 * Returns an unmodifiable list of the inputs available for this benchmark.
	 * @return an unmodifiable list of the inputs available for this benchmark
	 */
	public List<Dataset> inputs();

	/**
	 * Returns a human-readable name for this benchmark.
	 * @return a human-readable name for this benchmark
	 */
	@Override
	public String toString();

	/**
	 * A set of data a benchmark can run with.
	 *
	 * This class uses the builder pattern, despite not having many fields to
	 * initialize, to support future expansion with optional parameters (e.g.,
	 * the expected values of stateful fields at the end of the benchmark)
	 * without requiring modifiation of other benchmarks or conflicts between
	 * overloaded constructors with different sets of optional features.
	 */
	public static final class Dataset {
		private final Input<Object> input;
		/**
		 * An Input that produces Buffers for a verifying Output created by the
		 * benchmark framework.  May be null.
		 */
		private final Input<Object> output;
		private final String name;
		private Dataset(Input<Object> input, Input<Object> output, String name) {
			this.input = input;
			this.output = output;
			this.name = name;
		}
		public static Builder builder() {
			return new Builder();
		}
		public static Builder builder(Dataset dataset) {
			return new Builder().name(dataset.name).input(dataset.input).output(dataset.output);
		}
		public static class Builder {
			private Input<Object> input;
			private Input<Object> output;
			private String name;
			private Builder() {
			}
			public Builder name(String name) {
				this.name = name;
				return this;
			}
			public Builder input(Input<Object> input) {
				this.input = input;
				return this;
			}
			public Builder output(Input<Object> output) {
				this.output = output;
				return this;
			}
			public Dataset build() {
				return new Dataset(input, output, name);
			}
		}
		public Input<Object> input() {
			return input;
		}
		public Input<Object> output() {
			return output;
		}
		@Override
		public String toString() {
			return name;
		}
	}
}
