package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import java.util.Set;

/**
 * A fusion strategy using a set of boolean parameters.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/15/2014
 */
public final class BitsetFusionStrategy implements FusionStrategy {
	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		for (Worker<?, ?> w : workers)
			if (!Workers.isPeeking(w))
				builder.addParameter(Configuration.SwitchParameter.create("fuse"+Workers.getIdentifier(w), true));
	}

	@Override
	public boolean fuseUpward(ActorGroup group, Configuration config) {
		String paramName = String.format("fuse%d", group.id());
		Configuration.SwitchParameter<Boolean> param = config.getParameter(paramName, Configuration.SwitchParameter.class, Boolean.class);
		return param.getValue();
	}
}
