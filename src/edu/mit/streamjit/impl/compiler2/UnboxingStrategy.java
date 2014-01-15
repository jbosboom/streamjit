package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import java.util.Set;

/**
 * A strategy for unboxing. Such a strategy involves making parameters and
 * interpreting them.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/15/2014
 */
public interface UnboxingStrategy {
	/**
	 * Adds parameters used by this strategy to the given builder.
	 * @param workers the workers the configuration is being built for
	 * @param builder the builder
	 */
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder);

	/**
	 * Returns whether this storage should be unboxed.
	 * @param storage the storage to (maybe) unbox
	 * @param config config the configuration (will contain parameters created in
	 * makeParameters)
	 * @return whether the storage should be unboxed.
	 */
	public boolean unboxStorage(Storage storage, Configuration config);

	/**
	 * Returns whether this actor's input should be unboxed.
	 * @param actor the actor to (maybe) unbox the input of
	 * @param config config the configuration (will contain parameters created in
	 * makeParameters)
	 * @return whether the actor's input should be unboxed.
	 */
	public boolean unboxInput(WorkerActor actor, Configuration config);

	/**
	 * Returns whether this actor's output should be unboxed.
	 * @param actor the actor to (maybe) unbox the output of
	 * @param config config the configuration (will contain parameters created in
	 * makeParameters)
	 * @return whether the actor's output should be unboxed.
	 */
	public boolean unboxOutput(WorkerActor actor, Configuration config);
}
