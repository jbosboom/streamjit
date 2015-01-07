package edu.mit.streamjit.impl.distributed.runtimer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.ConfigurationManager;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel;

/**
 * Assumes the cluster environment is homogeneous.
 * 
 * @author sumanan
 * @since 7 Jan, 2015
 */
public class GraphPropertyPrognosticator implements ConfigurationPrognosticator {

	private final StreamJitApp<?, ?> app;

	private final ConfigurationManager cfgManager;

	public GraphPropertyPrognosticator(StreamJitApp<?, ?> app,
			ConfigurationManager cfgManager) {
		this.app = app;
		this.cfgManager = cfgManager;
	}

	@Override
	public boolean prognosticate(Configuration config) {
		if (!cfgManager.newConfiguration(config))
			return false;
		if (bigToSmallBlobRatio() > 10)
			return false;
		if (loadRatio() > 5)
			return false;
		return true;
	}

	/**
	 * @return The ratio between the number of workers in the largest blob and
	 *         the number of workers in the smallest blob.
	 */
	private float bigToSmallBlobRatio() {
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		int currentBlobSize;
		for (List<Set<Worker<?, ?>>> blobList : app.partitionsMachineMap
				.values()) {
			for (Set<Worker<?, ?>> blobWorkers : blobList) {
				currentBlobSize = blobWorkers.size();
				min = Math.min(min, currentBlobSize);
				max = Math.max(max, currentBlobSize);
			}
		}
		float blobRatio = ((float) max) / min;
		System.out.println("blobRatio - " + blobRatio);
		return blobRatio;
	}

	/**
	 * @return The ratio between the highest number of workers assigned to a
	 *         machine and the lowest number of workers assigned to a machine.
	 */
	private float loadRatio() {
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		int workersInCurrentNode;
		for (List<Set<Worker<?, ?>>> blobList : app.partitionsMachineMap
				.values()) {
			workersInCurrentNode = 0;
			for (Set<Worker<?, ?>> blobWorkers : blobList) {
				workersInCurrentNode += blobWorkers.size();
			}
			min = Math.min(min, workersInCurrentNode);
			max = Math.max(max, workersInCurrentNode);
		}
		float loadRatio = ((float) max) / min;
		System.out.println("loadRatio - " + loadRatio);
		return loadRatio;
	}

	/**
	 * @return The ratio between the total number of blobs to the total nodes.
	 */
	private float blobToNodeRatio() {
		int nodes = 0;
		int blobs = 0;
		for (List<Set<Worker<?, ?>>> blobList : app.partitionsMachineMap
				.values()) {
			nodes++;
			blobs += blobList.size();
		}
		float blobNodeRatio = ((float) blobs) / nodes;
		return blobNodeRatio;
	}

	/**
	 * @return The ratio between the total channels in the stream graph to the
	 *         {@link BoundaryChannel} in the current configuration.
	 */
	private float totalToBoundaryChannelRatio() {
		int totalChannels = 0;
		int boundaryChannels = 0;
		for (Integer machineID : app.partitionsMachineMap.keySet()) {
			List<Set<Worker<?, ?>>> blobList = app.partitionsMachineMap
					.get(machineID);
			Set<Worker<?, ?>> allWorkers = new HashSet<>();
			for (Set<Worker<?, ?>> blobWorkers : blobList) {
				allWorkers.addAll(blobWorkers);
			}

			for (Worker<?, ?> w : allWorkers) {
				for (Worker<?, ?> succ : Workers.getSuccessors(w)) {
					totalChannels++;
					if (!allWorkers.contains(succ))
						boundaryChannels++;
				}
			}
		}
		float boundaryChannelRatio = ((float) totalChannels) / boundaryChannels;
		return boundaryChannelRatio;
	}
}