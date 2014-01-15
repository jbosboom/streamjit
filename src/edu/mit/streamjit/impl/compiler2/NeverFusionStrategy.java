package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import java.util.Set;

/**
 * Never fuses.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/15/2014
 */
public final class NeverFusionStrategy implements FusionStrategy {
	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		//no parameters necessary
	}

	@Override
	public boolean fuseUpward(ActorGroup group, Configuration config) {
		return false;
	}
}
