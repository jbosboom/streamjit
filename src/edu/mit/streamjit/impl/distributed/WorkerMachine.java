package edu.mit.streamjit.impl.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.*;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.PartitionManager.AbstractPartitionManager;

/**
 * This class implements one type of search space. Adds "worker to machine"
 * mapping as tuning parameter for all workers and tune those. A naive way. When
 * opentuner gives back a new configuration following steps are carried to
 * determine the blobs.
 * <ol>
 * <li>Group the workers which are assigned to a particular machine.
 * <li>Find connected components from that group of workers.
 * <li>If a connected component contain a splitter and the corresponding joiner
 * but some workers in the spilitjoin, split that connected componenet into two
 * in order to avoid cycles.
 * <li>Assign each final connected components to each blob.
 * </ol>
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jan 16, 2014
 * 
 */
public final class WorkerMachine extends AbstractPartitionManager {

	private final Set<Worker<?, ?>> workerset;

	public WorkerMachine(StreamJitApp app) {
		super(app);
		this.workerset = Workers.getAllWorkersInGraph(app.source);
	}

	@Override
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers,
			int noOfMachines) {
		checkArgument(noOfMachines > 0, String.format(
				"noOfMachines = %d, It must be > 0", noOfMachines));
		Configuration.Builder builder = Configuration.builder();
		List<Integer> machinelist = new ArrayList<>(noOfMachines);
		for (int i = 1; i <= noOfMachines; i++)
			machinelist.add(i);

		for (Worker<?, ?> w : workers) {
			Parameter p = new Configuration.SwitchParameter<Integer>(
					getParamName(Workers.getIdentifier(w)), Integer.class, 1,
					machinelist);
			builder.addParameter(p);
		}

		// This parameter cannot be tuned. Its added here because we need this
		// parameter to run the app.
		// TODO: Consider using partition parameter and extradata to store this
		// kind of not tunable data.
		IntParameter noOfMachinesParam = new IntParameter("noOfMachines",
				noOfMachines, noOfMachines, noOfMachines);

		builder.addParameter(noOfMachinesParam);
		return builder.build();
	}

	public Map<Integer, List<Set<Worker<?, ?>>>> partitionMap(
			Configuration config) {

		Map<Integer, Set<Worker<?, ?>>> partition = new HashMap<>();
		for (Worker<?, ?> w : workerset) {
			SwitchParameter<Integer> w2m = config.getParameter(
					getParamName(Workers.getIdentifier(w)),
					SwitchParameter.class);
			int machine = w2m.getValue();

			if (!partition.containsKey(machine)) {
				Set<Worker<?, ?>> set = new HashSet<>();
				partition.put(machine, set);
			}
			partition.get(machine).add(w);
		}

		Map<Integer, List<Set<Worker<?, ?>>>> machineWorkerMap = new HashMap<>();
		for (int machine : partition.keySet()) {
			List<Set<Worker<?, ?>>> cycleMinimizedBlobs = new ArrayList<>();
			List<Set<Worker<?, ?>>> machineBlobs = getConnectedComponents(partition
					.get(machine));
			{
				for (Set<Worker<?, ?>> blobWorkers : machineBlobs) {
					cycleMinimizedBlobs.addAll(breakCycles(blobWorkers));
				}
			}
			machineWorkerMap.put(machine, cycleMinimizedBlobs);
		}
		return machineWorkerMap;
	}
}
