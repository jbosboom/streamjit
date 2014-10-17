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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.interp.Interpreter;
import edu.mit.streamjit.partitioner.AbstractPartitioner;

/**
 * ConfigurationManager deals with {@link Configuration}. Mainly, It does
 * following two tasks.
 * <ol>
 * <li>Generates configuration for with appropriate tuning parameters for
 * tuning.
 * <li>Dispatch the configuration given by the open tuner and make blobs
 * accordingly.
 * </ol>
 * 
 * One can implement this interface to try different search space designs as
 * they want.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jan 16, 2014
 * 
 */
public interface ConfigurationManager {

	/**
	 * Generates default configuration with all tuning parameters for tuning.
	 * 
	 * @param streamGraph
	 * @param source
	 * @param sink
	 * @param noOfMachines
	 * @return
	 */
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers,
			int noOfMachines);

	/**
	 * When opentuner gives a new configuration, this method may be called to
	 * interpret the configuration and execute the steramjit app with the new
	 * configuration.
	 * 
	 * @param config
	 *            configuration from opentuner.
	 * @return true iff valid configuration is passed.
	 */
	public boolean newConfiguration(Configuration config);

	/**
	 * Generates static information of the app that is needed by steramnodes.
	 * This configuration will be sent to streamnodes when setting up a new app
	 * for execution (Only once).
	 * 
	 * @return static information of the app that is needed by steramnodes.
	 */
	public Configuration getStaticConfiguration();

	/**
	 * For every reconfiguration, this method may be called by the appropriate
	 * class to get new configuration information that can be sent to all
	 * participating {@link StreamNode}s.
	 * 
	 * @return new partition information
	 */
	public Configuration getDynamicConfiguration();

	/**
	 * Implements the functions those can be called by runtimer to send
	 * configuration information to streamnodes.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Jan 17, 2014
	 */
	public static abstract class AbstractConfigurationManager
			implements
				ConfigurationManager {

		protected final StreamJitApp app;

		AbstractConfigurationManager(StreamJitApp app) {
			this.app = app;
		}

		@Override
		public Configuration getStaticConfiguration() {
			Configuration.Builder builder = Configuration.builder();
			builder.putExtraData(GlobalConstants.JARFILE_PATH, app.jarFilePath)
					.putExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME,
							app.topLevelClass);
			return builder.build();
		}

		@Override
		public Configuration getDynamicConfiguration() {
			Configuration.Builder builder = Configuration.builder();

			Map<Integer, Integer> coresPerMachine = new HashMap<>();
			for (Entry<Integer, List<Set<Worker<?, ?>>>> machine : app.partitionsMachineMap
					.entrySet()) {
				coresPerMachine
						.put(machine.getKey(), machine.getValue().size());
			}

			PartitionParameter.Builder partParam = PartitionParameter.builder(
					GlobalConstants.PARTITION, coresPerMachine);

			BlobFactory factory = new Interpreter.InterpreterBlobFactory();
			partParam.addBlobFactory(factory);

			app.blobtoMachineMap = new HashMap<>();

			for (Integer machineID : app.partitionsMachineMap.keySet()) {
				List<Set<Worker<?, ?>>> blobList = app.partitionsMachineMap
						.get(machineID);
				for (Set<Worker<?, ?>> blobWorkers : blobList) {
					// TODO: One core per blob. Need to change this.
					partParam.addBlob(machineID, 1, factory, blobWorkers);

					// TODO: Temp fix to build.
					Token t = Utils.getblobID(blobWorkers);
					app.blobtoMachineMap.put(t, machineID);
				}
			}

			builder.addParameter(partParam.build());
			if (app.blobConfiguration != null)
				builder.addSubconfiguration("blobConfigs",
						app.blobConfiguration);
			return builder.build();
		}

		/**
		 * Copied form {@link AbstractPartitioner} class. But modified to
		 * support nested splitjoiners.</p> Returns all {@link Worker}s in a
		 * splitjoin.
		 * 
		 * @param splitter
		 * @return Returns all {@link Filter}s in a splitjoin.
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

		protected String getParamName(Integer id) {
			assert id > -1 : "Worker id cannot be negative";
			return String.format("worker%dtomachine", id);
		}

		/**
		 * Goes through all workers in workerset which is passed as argument,
		 * find the workers which are interconnected and group them as a blob
		 * workers. i.e., Group the workers which are connected.
		 * <p>
		 * TODO: If any dynamic edges exists then should create interpreter
		 * blob.
		 * 
		 * @param workerset
		 * @return list of workers set which contains interconnected workers.
		 *         Each worker set in the list is supposed to run in an
		 *         individual blob.
		 */
		protected List<Set<Worker<?, ?>>> getConnectedComponents(
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
		 * check for the above mention condition. If cycles exists, split then
		 * in to several blobs.
		 * 
		 * @param blobworkers
		 * @return
		 */
		protected List<Set<Worker<?, ?>>> breakCycles(
				Set<Worker<?, ?>> blobworkers) {
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
					ret.add(getSplitterReachables(s, blobworkers,
							rfctrSplitJoin));
				}
			}
			ret.addAll(getConnectedComponents(blobworkers));
			return ret;
		}

		/**
		 * Goes through the passed set of workers, add workers those are
		 * reachable from the splitter s, but not any conflicting splitter or
		 * joiner.
		 * <p>
		 * This function has side effect. Modifies the argument.
		 * 
		 * @param s
		 * @param blobworkers
		 * @return
		 */
		protected Set<Worker<?, ?>> getSplitterReachables(Splitter<?, ?> s,
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
					process(succ, blobworkers, rfctrSplitJoin, exclude, queue,
							ret);
				}

				for (Worker<?, ?> pred : Workers.getPredecessors(wrkr)) {
					process(pred, blobworkers, rfctrSplitJoin, exclude, queue,
							ret);
				}
			}
			return ret;
		}

		/**
		 * Since the code in this method repeated in two places in
		 * getSplitterReachables() method, It is re-factored into a private
		 * method to avoid code duplication.
		 */
		protected void process(Worker<?, ?> wrkr,
				Set<Worker<?, ?>> blobworkers,
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

		protected Set<Splitter<?, ?>> getSplitters(Set<Worker<?, ?>> blobworkers) {
			Set<Splitter<?, ?>> splitterSet = new HashSet<>();
			for (Worker<?, ?> w : blobworkers) {
				if (w instanceof Splitter<?, ?>) {
					splitterSet.add((Splitter<?, ?>) w);
				}
			}
			return splitterSet;
		}

	}
}
