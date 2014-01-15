package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import java.util.Set;

/**
 * A strategy for using ActorGroups.  Such a strategy involves making
 * parameters and interpreting them.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/14/2014
 */
public interface FusionStrategy {
	/**
	 * Adds parameters used by this strategy to the given builder.
	 * @param workers the workers the configuration is being built for
	 * @param builder the builder
	 */
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder);

	/**
	 * Returns whether the given group should be fused into its predecessor.
	 * @param group the group to (maybe) fuse
	 * @param config the configuration  (will contain parameters created in
	 * makeParameters)
	 * @return whether the group should be fused
	 */
	public boolean fuseUpward(ActorGroup group, Configuration config);
}
