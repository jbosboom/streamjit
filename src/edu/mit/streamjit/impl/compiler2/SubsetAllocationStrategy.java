package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An AllocationStrategy using two parameters per worker: a core count and an
 * ordered list (permutation) of cores to assign to, effectively naming a subset
 * of the cores (but note there are more permutations than subsets).  The group
 * is then divided equally among those cores.
 *
 * This strategy was Jason Ansel's idea, briefly expressed in a conference call.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/2/2014
 */
public class SubsetAllocationStrategy implements AllocationStrategy {
	private final int maxNumCores;
	public SubsetAllocationStrategy(int maxNumCores) {
		this.maxNumCores = maxNumCores;
	}
	@Override
	public int maxNumCores() {
		return maxNumCores;
	}

	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		ImmutableList.Builder<Integer> universeBuilder = ImmutableList.builder();
		for (int i = 0; i < maxNumCores(); ++i)
			universeBuilder.add(i);
		ImmutableList<Integer> universe = universeBuilder.build();

		for (Worker<?, ?> w : workers) {
			int id = Workers.getIdentifier(w);
			builder.addParameter(new Configuration.IntParameter("Group"+id+"CoreCount", 1, maxNumCores(), maxNumCores()));
			builder.addParameter(new Configuration.PermutationParameter<>("Group"+id+"CoreOrder", Integer.class, universe));
		}
	}

	@Override
	public void allocateGroup(ActorGroup group, Range<Integer> iterations, List<Core> cores, Configuration config) {
		int id = group.id();
		int numCores = config.getParameter("Group"+id+"CoreCount", Configuration.IntParameter.class).getValue();
		Configuration.PermutationParameter<Integer> coreOrderParam = config.getParameter("Group"+id+"CoreOrder", Configuration.PermutationParameter.class, Integer.class);
		ImmutableList<? extends Integer> coreOrder = coreOrderParam.getUniverse();

		List<Core> subset = new ArrayList<Core>(numCores);
		for (int i = 0; i < coreOrder.size() && subset.size() < numCores; ++i)
			if (coreOrder.get(i) < cores.size())
				subset.add(cores.get(coreOrder.get(i)));
		//We pass a null config to ensure it doesn't depend on the config.
		new FullDataParallelAllocationStrategy(numCores).allocateGroup(group, iterations, cores, null);
	}
}
