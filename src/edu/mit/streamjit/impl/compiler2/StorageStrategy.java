package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import java.util.Set;

/**
 * A strategy for creating ConcreteStorage.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/1/2014
 */
public interface StorageStrategy {
	/**
	 * Adds parameters used by this strategy to the given builder.
	 * @param workers the workers the configuration is being built for
	 * @param builder the builder
	 */
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder);

	public StorageFactory asFactory(Configuration config);
}
