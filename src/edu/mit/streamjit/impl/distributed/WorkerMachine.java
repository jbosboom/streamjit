package edu.mit.streamjit.impl.distributed;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.AbstractDrainer.BlobGraph;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.distributed.ConfigurationManager.AbstractConfigurationManager;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.partitioner.AbstractPartitioner;

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
		// TODO Auto-generated method stub
		return null;
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
			SwitchParameter<Integer> w2m = config.getParameter(String.format(
					"worker%dtomachine", Workers.getIdentifier(w)),
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

	/**
	 * Goes through all workers in workerset which is passed as argument, find
	 * the workers which are interconnected and group them as a blob workers.
	 * i.e., Group the workers which are connected.
	 * <p>
	 * TODO: If any dynamic edges exists then should create interpreter blob.
	 * 
	 * @param workerset
	 * @return list of workers set which contains interconnected workers. Each
	 *         worker set in the list is supposed to run in an individual blob.
	 */
	private List<Set<Worker<?, ?>>> getConnectedComponents(
			Set<Worker<?, ?>> workerset) {
		List<Set<Worker<?, ?>>> ret = new ArrayList<Set<Worker<?, ?>>>();
		while (!workerset.isEmpty()) {
			Deque<Worker<?, ?>> queue = new ArrayDeque<>();
			Set<Worker<?, ?>> blobworkers = new HashSet<>();
			Worker<?, ?> w = workerset.iterator().next();
			blobworkers.add(w);
			workerset.remove(w);
			queue.offer(w);
			while (!queue.isEmpty()) {
				Worker<?, ?> wrkr = queue.poll();
				for (Worker<?, ?> succ : Workers.getSuccessors(wrkr)) {
					if (workerset.contains(succ)) {
						blobworkers.add(succ);
						workerset.remove(succ);
						queue.offer(succ);
					}
				}

				for (Worker<?, ?> pred : Workers.getPredecessors(wrkr)) {
					if (workerset.contains(pred)) {
						blobworkers.add(pred);
						workerset.remove(pred);
						queue.offer(pred);
					}
				}
			}
			ret.add(blobworkers);
		}
		return ret;
	}

	/**
	 * Cycles can occur iff splitter and joiner happened to fall into a blob
	 * while some workers of that splitjoin falls into other blob. Here, we
	 * check for the above mention condition. If cycles exists, split then in to
	 * several blobs.
	 * 
	 * @param blobworkers
	 * @return
	 */
	private List<Set<Worker<?, ?>>> breakCycles(Set<Worker<?, ?>> blobworkers) {
		Map<Splitter<?, ?>, Joiner<?, ?>> rfctrSplitJoin = new HashMap<>();
		Set<Splitter<?, ?>> splitterSet = getSplitters(blobworkers);
		for (Splitter<?, ?> s : splitterSet) {
			Joiner<?, ?> j = getJoiner(s);
			if (blobworkers.contains(j)) {
				Set<Worker<?, ?>> childWorkers = new HashSet<>();
				getAllChildWorkers(s, childWorkers);
				if (!blobworkers.containsAll(childWorkers)) {
					rfctrSplitJoin.put(s, j);
				}
			}
		}

		List<Set<Worker<?, ?>>> ret = new ArrayList<>();

		for (Splitter<?, ?> s : rfctrSplitJoin.keySet()) {
			if (blobworkers.contains(s)) {
				ret.add(getSplitterReachables(s, blobworkers, rfctrSplitJoin));
			}
		}
		ret.addAll(getConnectedComponents(blobworkers));
		return ret;
	}

	/**
	 * Goes through the passed set of workers, add workers those are reachable
	 * from the splitter s, but not any conflicting splitter or joiner.
	 * <p>
	 * This function has side effect. Modifies the argument.
	 * 
	 * @param s
	 * @param blobworkers
	 * @return
	 */
	private Set<Worker<?, ?>> getSplitterReachables(Splitter<?, ?> s,
			Set<Worker<?, ?>> blobworkers,
			Map<Splitter<?, ?>, Joiner<?, ?>> rfctrSplitJoin) {
		assert blobworkers.contains(s) : "Splitter s in not in blobworkers";
		Set<Worker<?, ?>> ret = new HashSet<>();
		Set<Worker<?, ?>> exclude = new HashSet<>();
		Deque<Worker<?, ?>> queue = new ArrayDeque<>();
		ret.add(s);
		exclude.add(rfctrSplitJoin.get(s));
		blobworkers.remove(s);
		queue.offer(s);
		while (!queue.isEmpty()) {
			Worker<?, ?> wrkr = queue.poll();
			for (Worker<?, ?> succ : Workers.getSuccessors(wrkr)) {
				process(succ, blobworkers, rfctrSplitJoin, exclude, queue, ret);
			}

			for (Worker<?, ?> pred : Workers.getPredecessors(wrkr)) {
				process(pred, blobworkers, rfctrSplitJoin, exclude, queue, ret);
			}
		}
		return ret;
	}

	/**
	 * Since the code in this method repeated in two places in
	 * getSplitterReachables() method, It is re-factored into a private method
	 * to avoid code duplication.
	 */
	private void process(Worker<?, ?> wrkr, Set<Worker<?, ?>> blobworkers,
			Map<Splitter<?, ?>, Joiner<?, ?>> rfctrSplitJoin,
			Set<Worker<?, ?>> exclude, Deque<Worker<?, ?>> queue,
			Set<Worker<?, ?>> ret) {
		if (blobworkers.contains(wrkr) && !exclude.contains(wrkr)) {
			ret.add(wrkr);
			blobworkers.remove(wrkr);
			queue.offer(wrkr);

			for (Entry<Splitter<?, ?>, Joiner<?, ?>> e : rfctrSplitJoin
					.entrySet()) {
				if (e.getValue().equals(wrkr)) {
					exclude.add(e.getKey());
					break;
				} else if (e.getKey().equals(wrkr)) {
					exclude.add(e.getValue());
					break;
				}
			}
		}
	}

	/**
	 * Copied form {@link AbstractPartitioner} class. But modified to support
	 * nested splitjoiners.</p> Returns all {@link Filter}s in a splitjoin. Does
	 * not include the splitter or the joiner.
	 * 
	 * @param splitter
	 * @return Returns all {@link Filter}s in a splitjoin. Does not include
	 *         splitter or joiner.
	 */
	protected void getAllChildWorkers(Splitter<?, ?> splitter,
			Set<Worker<?, ?>> childWorkers) {
		childWorkers.add(splitter);
		Joiner<?, ?> joiner = getJoiner(splitter);
		Worker<?, ?> cur;
		for (Worker<?, ?> childWorker : Workers.getSuccessors(splitter)) {
			cur = childWorker;
			while (cur != joiner) {
				if (cur instanceof Filter<?, ?>)
					childWorkers.add(cur);
				else if (cur instanceof Splitter<?, ?>) {
					getAllChildWorkers((Splitter<?, ?>) cur, childWorkers);
					cur = getJoiner((Splitter<?, ?>) cur);
				} else
					throw new IllegalStateException(
							"Some thing wrong in the algorithm.");

				assert Workers.getSuccessors(cur).size() == 1 : "Illegal State encounted : cur can only be either a filter or a joner";
				cur = Workers.getSuccessors(cur).get(0);
			}
		}
		childWorkers.add(joiner);
	}

	private Set<Splitter<?, ?>> getSplitters(Set<Worker<?, ?>> blobworkers) {
		Set<Splitter<?, ?>> splitterSet = new HashSet<>();
		for (Worker<?, ?> w : blobworkers) {
			if (w instanceof Splitter<?, ?>) {
				splitterSet.add((Splitter<?, ?>) w);
			}
		}
		return splitterSet;
	}

	/**
	 * Find and returns the corresponding {@link Joiner} for the passed
	 * {@link Splitter}.
	 * 
	 * @param splitter
	 *            : {@link Splitter} that needs it's {@link Joiner}.
	 * @return Corresponding {@link Joiner} of the passed {@link Splitter}.
	 */
	protected Joiner<?, ?> getJoiner(Splitter<?, ?> splitter) {
		Worker<?, ?> cur = Workers.getSuccessors(splitter).get(0);
		int innerSplitjoinCount = 0;
		while (!(cur instanceof Joiner<?, ?>) || innerSplitjoinCount != 0) {
			if (cur instanceof Splitter<?, ?>)
				innerSplitjoinCount++;
			if (cur instanceof Joiner<?, ?>)
				innerSplitjoinCount--;
			assert innerSplitjoinCount >= 0 : "Joiner Count is more than splitter count. Check the algorithm";
			cur = Workers.getSuccessors(cur).get(0);
		}
		assert cur instanceof Joiner<?, ?> : "Error in algorithm. Not returning a Joiner";
		return (Joiner<?, ?>) cur;
	}

	@Override
	public Configuration getStaticConfiguration() {
		Configuration.Builder builder = Configuration.builder();
		builder.putExtraData(GlobalConstants.JARFILE_PATH, app.jarFilePath)
				.putExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME,
						app.topLevelClass);
		return builder.build();
	}

}
