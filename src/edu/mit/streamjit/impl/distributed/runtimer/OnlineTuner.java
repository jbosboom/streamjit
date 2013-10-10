package edu.mit.streamjit.impl.distributed.runtimer;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.AbstractDrainer.BlobGraph;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.tuner.OpenTuner;
import edu.mit.streamjit.tuner.TCPTuner;
import edu.mit.streamjit.util.json.Jsonifiers;

/**
 * Online tuner does continues learning.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Oct 8, 2013
 */
public class OnlineTuner implements Runnable {
	AbstractDrainer drainer;
	Controller controller;
	OpenTuner tuner;
	StreamJitApp app;

	public OnlineTuner(AbstractDrainer drainer, Controller controller,
			StreamJitApp app) {
		this.drainer = drainer;
		this.controller = controller;
		this.app = app;
		this.tuner = new TCPTuner();
	}

	@Override
	public void run() {
		int tryCount = 0;
		try {
			tuner.startTuner(String.format(
					"lib%sopentuner%sstreamjit%sstreamjit2.py", File.separator,
					File.separator, File.separator));

			tuner.writeLine("program");
			tuner.writeLine(app.topLevelClass);

			tuner.writeLine("confg");
			String s = getConfigurationString(app.blobConfiguration);
			tuner.writeLine(s);

			System.out.println("New tune run.............");
			while (true) {
				String pythonDict = tuner.readLine();
				if (pythonDict.equals("Completed")) {
					String finalConfg = tuner.readLine();
					System.out.println("Tuning finished");
					break;
				}

				System.out
						.println("----------------------------------------------");
				System.out.println(tryCount++);
				Configuration config = rebuildConfiguraion(pythonDict,
						app.blobConfiguration);
				try {
					if (!app.newConfiguration(config)) {
						tuner.writeLine("-1");
						continue;
					}

					boolean state = drainer.startDraining(false);
					if (!state) {
						System.err
								.println("Final drain has already been called. no more tuning.");
						tuner.writeLine("exit");
						break;
					}

					drainer.awaitDrainedIntrmdiate();
					drainer.setBlobGraph(app.blobGraph1);

					controller.reconfigure();

					Thread.sleep(10000);

					double time = controller.getperformanceTime();
					System.out.println("Execution time is " + time
							+ " milli seconds");
					tuner.writeLine(new Double(time).toString());
				} catch (Exception ex) {
					System.err
							.println("Couldn't compile the stream graph with this configuration");
					tuner.writeLine("-1");
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Reads the configuration and returns a map of nodeID to list of workers
	 * set which are assigned to the node. value of the returned map is list of
	 * worker set where each worker set is individual blob.
	 * 
	 * @param config
	 * @param workerset
	 * @return map of nodeID to list of workers set which are assigned to the
	 *         node. value is list of worker set where each set is individual
	 *         blob.
	 */
	private Map<Integer, List<Set<Worker<?, ?>>>> getMachineWorkerMap(
			Configuration config, Worker<?, ?> source) {

		ImmutableSet<Worker<?, ?>> workerset = Workers
				.getAllWorkersInGraph(source);

		Map<Integer, Set<Worker<?, ?>>> partition = new HashMap<>();
		for (Worker<?, ?> w : workerset) {
			IntParameter w2m = config.getParameter(String.format(
					"worker%dtomachine", Workers.getIdentifier(w)),
					IntParameter.class);
			int machine = w2m.getValue();

			if (!partition.containsKey(machine)) {
				Set<Worker<?, ?>> set = new HashSet<>();
				partition.put(machine, set);
			}
			partition.get(machine).add(w);
		}

		Map<Integer, List<Set<Worker<?, ?>>>> machineWorkerMap = new HashMap<>();
		for (int machine : partition.keySet()) {
			machineWorkerMap.put(machine, getBlobs(partition.get(machine)));
		}
		return machineWorkerMap;
	}

	private boolean isValid(
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap,
			BlobGraph bg) {
		List<Set<Worker<?, ?>>> partitionList = new ArrayList<>();
		for (List<Set<Worker<?, ?>>> lst : partitionsMachineMap.values()) {
			partitionList.addAll(lst);
		}
		try {
			bg = new BlobGraph(partitionList);

		} catch (StreamCompilationFailedException ex) {
			System.err.print("Cycles found in the worker->blob assignment");
			for (int machine : partitionsMachineMap.keySet()) {
				System.err.print("\nMachine - " + machine);
				for (Set<Worker<?, ?>> blobworkers : partitionsMachineMap
						.get(machine)) {
					System.err.print("\n\tBlob worker set : ");
					for (Worker<?, ?> w : blobworkers) {
						System.err.print(Workers.getIdentifier(w) + " ");
					}
				}
			}
			System.err.println();
			return false;
		}
		return true;
	}

	/**
	 * Goes through all the workers assigned to a machine, find the workers
	 * which are interconnected and group them as a blob workers. i.e., Group
	 * the workers such that each group can be executed as a blob.
	 * <p>
	 * TODO: If any dynamic edges exists then should create interpreter blob.
	 * 
	 * @param workerset
	 * @return list of workers set which contains interconnected workers. Each
	 *         worker set in the list is supposed to run in an individual blob.
	 */
	private List<Set<Worker<?, ?>>> getBlobs(Set<Worker<?, ?>> workerset) {
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
	 * Creates a new {@link Configuration} from the received python dictionary
	 * string. This is not a good way to do.
	 * <p>
	 * TODO: Need to add a method to {@link Configuration} so that the
	 * configuration object can be updated from the python dict string. Now we
	 * are destructing the old confg object and recreating a new one every time.
	 * Not a appreciatable way.
	 * 
	 * @param pythonDict
	 *            Python dictionary string. Autotuner gives a dictionary of
	 *            features with trial values.
	 * @param config
	 *            Old configuration object.
	 * @return New configuration object with updated values from the pythonDict.
	 */
	private Configuration rebuildConfiguraion(String pythonDict,
			Configuration config) {
		// System.out.println(pythonDict);
		checkNotNull(pythonDict, "Received Python dictionary is null");
		pythonDict = pythonDict.replaceAll("u'", "");
		pythonDict = pythonDict.replaceAll("':", "");
		pythonDict = pythonDict.replaceAll("\\{", "");
		pythonDict = pythonDict.replaceAll("\\}", "");
		Splitter dictSplitter = Splitter.on(", ").omitEmptyStrings()
				.trimResults();
		Configuration.Builder builder = Configuration.builder();
		System.out.println("New parameter values from Opentuner...");
		for (String s : dictSplitter.split(pythonDict)) {
			String[] str = s.split(" ");
			if (str.length != 2)
				throw new AssertionError("Wrong python dictionary...");
			Parameter p = config.getParameter(str[0]);
			if (p == null)
				continue;
			// System.out.println(String.format("\t%s = %s", str[0], str[1]));
			if (p instanceof IntParameter) {
				IntParameter ip = (IntParameter) p;
				builder.addParameter(new IntParameter(ip.getName(),
						ip.getMin(), ip.getMax(), Integer.parseInt(str[1])));

			} else if (p instanceof SwitchParameter<?>) {
				SwitchParameter sp = (SwitchParameter) p;
				Class<?> type = sp.getGenericParameter();
				int val = Integer.parseInt(str[1]);
				SwitchParameter<?> sp1 = new SwitchParameter(sp.getName(),
						type, sp.getUniverse().get(val), sp.getUniverse());
				builder.addParameter(sp1);
			}

		}
		return builder.build();
	}

	private String getConfigurationString(Configuration cfg) {
		String s = Jsonifiers.toJson(cfg).toString();
		String s1 = s.replaceAll("__class__", "ttttt");
		String s2 = s1.replaceAll("class", "javaClassPath");
		String s3 = s2.replaceAll("ttttt", "__class__");
		return s3;
	}
}
