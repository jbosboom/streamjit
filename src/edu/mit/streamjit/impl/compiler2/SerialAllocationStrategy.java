package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.Range;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import java.util.List;
import java.util.Set;

/**
 * An AllocationStrategy that assigns all iterations of all groups to core 0.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 12/31/2013
 */
public final class SerialAllocationStrategy implements AllocationStrategy {
	private SerialAllocationStrategy() {}
	public static final AllocationStrategy INSTANCE = new SerialAllocationStrategy();

	@Override
	public int maxNumCores() {
		return 1;
	}

	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		//no parameters required
	}

	@Override
	public void allocateGroup(ActorGroup group, Range<Integer> iterations, List<Core> cores, Configuration config) {
		cores.get(0).allocate(group, iterations);
	}
}
