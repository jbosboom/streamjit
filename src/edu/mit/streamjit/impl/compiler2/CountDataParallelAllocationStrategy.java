package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.Range;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import java.util.List;
import java.util.Set;

/**
 * An AllocationStrategy that uses a count parameter to decide how many cores to
 * spread each group over.
 *
 * This doesn't make any attempt to efficiently bin-pack in the case where not
 * all cores are used (doing so would require either extra parameters for the
 * packing order or a work estimation heuristic).
 *
 * TODO: we could have the parameter range from 1 to maxNumCores()*2-1 to give
 * us a 50% chance of picking the maximum when choosing at random (other weights
 * are obviously possible too).
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/12/2014
 */
public class CountDataParallelAllocationStrategy implements AllocationStrategy {
	private final int maxNumCores;
	public CountDataParallelAllocationStrategy(int maxNumCores) {
		this.maxNumCores = maxNumCores;
	}

	@Override
	public int maxNumCores() {
		return maxNumCores;
	}

	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		for (Worker<?, ?> w : workers) {
			String name = String.format("Group%dCores", Workers.getIdentifier(w));
			builder.addParameter(new Configuration.IntParameter(name, 1, maxNumCores(), maxNumCores()));
		}
	}

	@Override
	public void allocateGroup(ActorGroup group, Range<Integer> iterations, List<Core> cores, Configuration config) {
		String name = String.format("Group%dCores", group.id());
		Configuration.IntParameter param = config.getParameter(name, Configuration.IntParameter.class);
		int count = Math.min(Math.min(cores.size(), maxNumCores()), param.getValue());
		assert count > 0 : count;
		new FullDataParallelAllocationStrategy(count).allocateGroup(group, iterations, cores.subList(0, count), config);
	}
}
