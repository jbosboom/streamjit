package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.math.DoubleMath;
import com.google.common.math.IntMath;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.util.CollectionUtils;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * An allocation strategy that gives each core a floating-point "share"
 * parameter in [0,1], then allocates the specified fraction of the total
 * iterations to those cores with priority by descending order of share.
 *
 * This strategy favors space multiplexing.  Equal data-parallelization can only
 * occur if all N shares are approximately 1/N, as we do not normalize.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 12/31/2013
 */
public class DescendingShareAllocationStrategy implements AllocationStrategy {
	private final int maxNumCores;
	public DescendingShareAllocationStrategy(int maxNumCores) {
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
				String name = String.format("node%dcore%dshare", Workers.getIdentifier(w), i);
				builder.addParameter(new Configuration.FloatParameter(name, 0, 1, 1f/maxNumCores()));
			}
	}

	@Override
	public void allocateGroup(ActorGroup group, Range<Integer> iterations, List<Core> cores, Configuration config) {
		List<Float> shares = new ArrayList<>(cores.size());
		for (int core = 0; core < cores.size(); ++core) {
			String name = String.format("node%dcore%diter", group.id(), core);
			Configuration.FloatParameter parameter = config.getParameter(name, Configuration.FloatParameter.class);
			if (parameter == null)
				shares.add(0f);
			else
				shares.add(parameter.getValue());
		}

		assert iterations.lowerBoundType() == BoundType.CLOSED && iterations.upperBoundType() == BoundType.OPEN;
		int totalAvailable = iterations.upperEndpoint() - iterations.lowerEndpoint();
		while (!iterations.isEmpty()) {
			int max = CollectionUtils.maxIndex(shares);
			float share = shares.get(max);
			if (share == 0) break;
			int amount = DoubleMath.roundToInt(share * totalAvailable, RoundingMode.HALF_EVEN);
			int done = iterations.lowerEndpoint();
			Range<Integer> allocation = group.isStateful() ? iterations :
					iterations.intersection(Range.closedOpen(done, done + amount));
			cores.get(max).allocate(group, allocation);
			iterations = Range.closedOpen(allocation.upperEndpoint(), iterations.upperEndpoint());
			shares.set(max, 0f); //don't allocate to this core again
		}

		//If we have iterations left over not assigned to a core, spread them
		//evenly over all cores.
		if (!iterations.isEmpty()) {
			int perCore = IntMath.divide(iterations.upperEndpoint() - iterations.lowerEndpoint(), cores.size(), RoundingMode.CEILING);
			for (int i = 0; i < cores.size() && !iterations.isEmpty(); ++i) {
				int min = iterations.lowerEndpoint();
				Range<Integer> allocation = group.isStateful() ? iterations :
						iterations.intersection(Range.closedOpen(min, min + perCore));
				cores.get(i).allocate(group, allocation);
				iterations = Range.closedOpen(allocation.upperEndpoint(), iterations.upperEndpoint());
			}
		}
		assert iterations.isEmpty();
	}
}