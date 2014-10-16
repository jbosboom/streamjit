package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import java.util.Set;

/**
 * A removal strategy using a set of boolean parameters.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/15/2014
 */
public final class BitsetRemovalStrategy implements RemovalStrategy {
	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		for (Worker<?, ?> w : workers)
			if (Compiler2.REMOVABLE_WORKERS.contains(w.getClass()))
				builder.addParameter(Configuration.SwitchParameter.create("remove"+Workers.getIdentifier(w), true));
	}

	@Override
	public boolean remove(WorkerActor a, Configuration config) {
		Configuration.SwitchParameter<Boolean> param = config.getParameter("remove"+a.id(), Configuration.SwitchParameter.class, Boolean.class);
		return param.getValue();
	}
}
