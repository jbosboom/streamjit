package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.IOInfo;
import edu.mit.streamjit.impl.common.Workers;
import java.util.Set;

/**
 * An unboxing strategy using a set of boolean parameters.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/15/2014
 */
public final class BitsetUnboxingStrategy implements UnboxingStrategy {
	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		for (IOInfo i : IOInfo.allEdges(workers))
			builder.addParameter(Configuration.SwitchParameter.create("unboxStorage"+i.token().toString().replace(", ", "_"), i.isInternal()));
		for (Worker<?, ?> w : workers) {
			builder.addParameter(Configuration.SwitchParameter.create("unboxInput"+Workers.getIdentifier(w), true));
			builder.addParameter(Configuration.SwitchParameter.create("unboxOutput"+Workers.getIdentifier(w), true));
		}
	}

	@Override
	public boolean unboxStorage(Storage storage, Configuration config) {
		Configuration.SwitchParameter<Boolean> param = config.getParameter("unboxStorage"+storage.id().toString().replace(", ", "_"), Configuration.SwitchParameter.class, Boolean.class);
		return param.getValue();
	}

	@Override
	public boolean unboxInput(WorkerActor actor, Configuration config) {
		Configuration.SwitchParameter<Boolean> param = config.getParameter("unboxInput"+actor.id(), Configuration.SwitchParameter.class, Boolean.class);
		return param.getValue();
	}

	@Override
	public boolean unboxOutput(WorkerActor actor, Configuration config) {
		Configuration.SwitchParameter<Boolean> param = config.getParameter("unboxOutput"+actor.id(), Configuration.SwitchParameter.class, Boolean.class);
		return param.getValue();
	}
}
