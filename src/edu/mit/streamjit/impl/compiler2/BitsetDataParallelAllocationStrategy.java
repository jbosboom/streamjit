package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.Range;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An allocation strategy that maintains a bitset for each group,
 * data-parallelizing evenly over cores whose bits are set.
 *
 * This strategy favors space multiplexing.  Equal data-parallelization can only
 * occur if all N bits are set, and there are more ways to assign two groups to
 * N/2 cores each than both to all cores.  However, within each group's
 * assignment, cores receive equal allocations.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 12/31/2013
 */
public class BitsetDataParallelAllocationStrategy implements AllocationStrategy {
	private final int maxNumCores;
	public BitsetDataParallelAllocationStrategy(int maxNumCores) {
		this.maxNumCores = maxNumCores;
	}

	@Override
	public int maxNumCores() {
		return maxNumCores;
	}

	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		for (Worker<?, ?> w : workers)
			for (int i = 0; i < maxNumCores(); ++i) {
				String name = String.format("node%dcore%dallocate", Workers.getIdentifier(w), i);
				builder.addParameter(Configuration.SwitchParameter.create(name, true));
			}
	}

	@Override
	public void allocateGroup(ActorGroup group, Range<Integer> iterations, List<Core> cores, Configuration config) {
		int count = Math.min(cores.size(), maxNumCores());
		List<Core> enabled = new ArrayList<>(count);
		for (int i = 0; i < count; ++i) {
			String name = String.format("node%dcore%dallocate", group.id(), i);
			Configuration.SwitchParameter<Boolean> param = config.getParameter(name, Configuration.SwitchParameter.class, Boolean.class);
			if (param.getValue())
				enabled.add(cores.get(i));
		}
		new FullDataParallelAllocationStrategy(enabled.size()).allocateGroup(group, iterations, enabled, config);
	}
}
