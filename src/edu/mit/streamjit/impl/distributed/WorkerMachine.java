/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.AbstractDrainer.BlobGraph;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.distributed.ConfigurationManager.AbstractConfigurationManager;

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
public final class WorkerMachine extends AbstractConfigurationManager {

	WorkerMachine(StreamJitApp app) {
		super(app);
	}

	@Override
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers,
			int noOfMachines) {
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

	/**
	 * Builds partitionsMachineMap and {@link BlobGraph} from the new
	 * Configuration, and verifies for any cycles among blobs. If it is a valid
	 * configuration, (i.e., no cycles among the blobs), then {@link #app}
	 * object's member variables {@link StreamJitApp#blobConfiguration},
	 * {@link StreamJitApp#blobGraph} and
	 * {@link StreamJitApp#partitionsMachineMap} will be assigned according to
	 * reflect the new configuration, no changes otherwise.
	 * 
	 * @param config
	 *            New configuration form Opentuer.
	 * @return true iff no cycles among blobs
	 */
	@Override
	public boolean newConfiguration(Configuration config) {

		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = getMachineWorkerMap(
				config, app.source);
		try {
			app.varifyConfiguration(partitionsMachineMap);
		} catch (StreamCompilationFailedException ex) {
			return false;
		}
		app.blobConfiguration = config;
		return true;
	}

	/**
	 * Reads the configuration and returns a map of nodeID to list of set of
	 * workers (list of blob workers) which are assigned to the node. Value of
	 * the returned map is list of worker set where each worker set is an
	 * individual blob.
	 * 
	 * @param config
	 * @param workerset
	 * @return map of nodeID to list of set of workers which are assigned to the
	 *         node.
	 */
	private Map<Integer, List<Set<Worker<?, ?>>>> getMachineWorkerMap(
			Configuration config, Worker<?, ?> source) {

		ImmutableSet<Worker<?, ?>> workerset = Workers
				.getAllWorkersInGraph(source);

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
