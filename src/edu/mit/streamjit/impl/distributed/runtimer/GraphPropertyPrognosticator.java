package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel;
import edu.mit.streamjit.impl.distributed.common.Options;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.util.ConfigurationUtils;

/**
 * Assumes the cluster environment is homogeneous.
 * 
 * @author sumanan
 * @since 7 Jan, 2015
 */
public class GraphPropertyPrognosticator implements ConfigurationPrognosticator {

	private final StreamJitApp<?, ?> app;

	private final FileWriter writer;

	private final Set<List<Integer>> paths;

	public GraphPropertyPrognosticator(StreamJitApp<?, ?> app) {
		this.app = app;
		this.writer = Utils.fileWriter(String.format("%s%sGraphProperty.txt",
				app.name, File.separator));
		writeHeader(writer);
		paths = app.paths();
	}

	@Override
	public boolean prognosticate(Configuration config) {
		String cfgPrefix = ConfigurationUtils.getConfigPrefix(config);
		float bigToSmallBlobRatio = bigToSmallBlobRatio();
		float loadRatio = loadRatio();
		float blobToNodeRatio = blobToNodeRatio();
		float boundaryChannelRatio = totalToBoundaryChannelRatio();
		boolean hasCycle = hasCycle();
		try {
			writer.write(String.format("\n%6s\t\t", cfgPrefix));
			writer.write(String.format("%.2f\t\t", bigToSmallBlobRatio));
			writer.write(String.format("%.2f\t\t", loadRatio));
			writer.write(String.format("%.2f\t\t", blobToNodeRatio));
			writer.write(String.format("%.2f\t\t", boundaryChannelRatio));
			writer.write(String.format("%s\t\t", hasCycle ? "True" : "False"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return decide(bigToSmallBlobRatio, loadRatio, blobToNodeRatio,
				boundaryChannelRatio, hasCycle);
	}

	private boolean decide(float bigToSmallBlobRatio, float loadRatio,
			float blobToNodeRatio, float boundaryChannelRatio, boolean hasCycle) {
		StringBuilder s = new StringBuilder();
		boolean accept = true;
		if (Options.prognosticate) {
			if (Options.bigToSmallBlobRatio > 0
					&& bigToSmallBlobRatio > Options.bigToSmallBlobRatio) {
				s.append("1,");
				accept = false;
			}
			if (Options.loadRatio > 0 && loadRatio > Options.loadRatio) {
				s.append("2,");
				accept = false;
			}
			if (Options.blobToNodeRatio > 0
					&& blobToNodeRatio > Options.blobToNodeRatio) {
				s.append("3,");
				accept = false;
			}
			if (Options.boundaryChannelRatio > 0
					&& boundaryChannelRatio < Options.boundaryChannelRatio) {
				s.append("4,");
				accept = false;
			}
			if (hasCycle) {
				s.append("5,");
				accept = false;
			}
		}

		try {
			writer.write(String.format("%s\t\t",
					accept ? "Accepted" : s.toString()));
		} catch (IOException e) {

		}
		return accept;
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

	private static void writeHeader(FileWriter writer) {
		try {
			writer.write(String.format("%.7s", "cfgID"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "bigToSmallBlobRatio"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "loadRatio"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "blobToNodeRatio"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "BoundaryChannelRatio"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "hasCycles"));
			writer.write("\t\t");
			writer.write(String.format("%.7s", "A/R")); // Accepted or Rejected.
			writer.write("\t\t");
			writer.write(String.format("%.7s", "time"));
			// writer.write("\t\t");
			writer.flush();
		} catch (IOException e) {

		}
	}

	@Override
	public void time(double time) {
		try {
			writer.write(String.format("%.0f", time));
			writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private boolean hasCycle() {
		Set<List<Integer>> machinePaths = buildMachinePaths();
		for (List<Integer> path : machinePaths) {
			Set<Integer> machines = new HashSet<Integer>();
			for (int i = 0; i < path.size() - 1; i++) {
				int machine = path.get(i);
				if (machines.contains(machine))
					return true;
				machines.add(machine);

			}
		}
		return false;
	}

	private Set<List<Integer>> buildMachinePaths() {
		Set<List<Integer>> machinePaths = new HashSet<List<Integer>>();
		List<Integer> machinePath;
		for (List<Integer> path : paths) {
			machinePath = new LinkedList<Integer>();
			int curMachine = -1;
			for (Integer worker : path) {
				int machine = getAssignedMachine(worker);
				if (curMachine != machine) {
					machinePath.add(machine);
					curMachine = machine;
				}
			}
			machinePaths.add(machinePath);
		}
		return machinePaths;
	}

	private int getAssignedMachine(int workerID) {
		for (Integer machineID : app.partitionsMachineMap.keySet()) {
			for (Set<Worker<?, ?>> blobWorkers : app.partitionsMachineMap
					.get(machineID)) {
				for (Worker<?, ?> w : blobWorkers) {
					if (Workers.getIdentifier(w) == workerID)
						return machineID;
				}
			}
		}

		throw new IllegalArgumentException(String.format(
				"Worker-%d is not assigned to anyof the machines", workerID));
	}
}
