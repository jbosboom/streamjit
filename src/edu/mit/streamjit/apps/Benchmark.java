package edu.mit.streamjit.apps;

import edu.mit.streamjit.api.OneToOneElement;
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
	public List<Input> inputs();

	/**
	 * Returns a human-readable name for this benchmark.
	 * @return a human-readable name for this benchmark
	 */
	@Override
	public String toString();

	public interface Input {
		/**
		 * Returns a Buffer containing input for this benchmark.  Buffers are
		 * stateful objects, so this method always returns a new Buffer.
		 * @return a new Buffer containing input for this benchmark.
		 */
		public Buffer input();
		/**
		 * Returns a Buffer containing reference output for this input, or null
		 * if reference output is not available.  Buffers are
		 * stateful objects, so this method always returns a new Buffer.
		 * @return a new Buffer containing reference output for this input, or
		 * null
		 */
		public Buffer output();
		/**
		 * Returns a human-readable name for this input.
		 * @return a human-readable name for this input
		 */
		@Override
		public String toString();
	}
}
