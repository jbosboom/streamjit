package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.Range;
import com.google.common.math.IntMath;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import java.math.RoundingMode;
import java.util.List;
import java.util.Set;

/**
 * An allocation strategy that fully data-parallelizes over up to N cores.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 12/31/2013
 */
public final class FullDataParallelAllocationStrategy implements AllocationStrategy {
	private final int maxNumCores;
	public FullDataParallelAllocationStrategy(int maxNumCores) {
		checkArgument(maxNumCores >= 1);
		this.maxNumCores = maxNumCores;
	}

	@Override
	public int maxNumCores() {
		return maxNumCores;
	}

	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		//no parameters necessary
	}

	@Override
	public void allocateGroup(ActorGroup group, Range<Integer> iterations, List<Core> cores, Configuration config) {
		int coresSize = Math.min(cores.size(), maxNumCores);
		int perCore = IntMath.divide(iterations.upperEndpoint() - iterations.lowerEndpoint(), coresSize, RoundingMode.CEILING);
		for (int i = 0; i < coresSize && !iterations.isEmpty(); ++i) {
			int min = iterations.lowerEndpoint();
			Range<Integer> allocation = group.isStateful() ? iterations :
					iterations.intersection(Range.closedOpen(min, min + perCore));
			cores.get(i).allocate(group, allocation);
			iterations = Range.closedOpen(allocation.upperEndpoint(), iterations.upperEndpoint());
		}
		assert iterations.isEmpty();
	}
}
