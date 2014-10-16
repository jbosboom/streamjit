package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import java.util.Set;

/**
 * A strategy for replacing actors with index functions. Such a strategy
 * involves making parameters and interpreting them.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/15/2014
 */
public interface RemovalStrategy {
	/**
	 * Adds parameters used by this strategy to the given builder.
	 * @param workers the workers the configuration is being built for
	 * @param builder the builder
	 */
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder);

	/**
	 * Returns whether this WorkerActor should be removed from the graph and
	 * replaced with index functions.
	 * @param a the actor to (maybe) remove
	 * @param config the configuration (will contain parameters created in
	 * makeParameters)
	 * @return whether the actor should be removed
	 */
	public boolean remove(WorkerActor a, Configuration config);
}
