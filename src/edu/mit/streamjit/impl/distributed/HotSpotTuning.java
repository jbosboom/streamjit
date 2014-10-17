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

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.StreamVisitor;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.distributed.ConfigurationManager.AbstractConfigurationManager;
import edu.mit.streamjit.tuner.OfflineTuner;

public final class HotSpotTuning extends AbstractConfigurationManager {

	Map<Integer, List<Worker<?, ?>>> partitionGroup;
	Map<Splitter<?, ?>, Set<Worker<?, ?>>> skippedSplitters;

	public HotSpotTuning(StreamJitApp app) {
		super(app);
	}

	@Override
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers,
			int noOfMachines) {
		PickHotSpots visitor = new PickHotSpots(noOfMachines);
		app.streamGraph.visit(visitor);
		return visitor.builder.build();
	}

	@Override
	public boolean newConfiguration(Configuration config) {

		for (Parameter p : config.getParametersMap().values()) {
			if (p instanceof IntParameter) {
				IntParameter ip = (IntParameter) p;
				System.out.println(ip.getName() + " - " + ip.getValue());
			} else if (p instanceof SwitchParameter<?>) {
				SwitchParameter<?> sp = (SwitchParameter<?>) p;
				System.out.println(sp.getName() + " - " + sp.getValue());
			} else
				System.out.println(p.getName() + " - Unknown type");
		}

		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = getMachineWorkerMap(config);
		try {
			app.varifyConfiguration(partitionsMachineMap);
		} catch (StreamCompilationFailedException ex) {
			return false;
		}
		app.blobConfiguration = config;
		return true;
	}

	private Map<Integer, List<Set<Worker<?, ?>>>> getMachineWorkerMap(
			Configuration config) {
		Map<Integer, Set<Worker<?, ?>>> partition = new HashMap<>();

		for (Integer id : partitionGroup.keySet()) {
			int machine = getAssignedMachine(id, config, partition);

			int val;
			List<Worker<?, ?>> workerList = partitionGroup.get(id);
			IntParameter cutParam = (IntParameter) config.getParameter(String
					.format("worker%dcut", id));
			if (cutParam != null)
				val = cutParam.getValue();
			else
				val = 1;

			for (int i = 0; i < val; i++) {
				Worker<?, ?> w = workerList.get(i);
				if (skippedSplitters.containsKey(w)) {
					partition.get(machine).addAll(skippedSplitters.get(w));
				} else
					partition.get(machine).add(w);
			}

			if (val < workerList.size()) {
				Worker<?, ?> w = workerList.get(workerList.size() - 1);
				if (skippedSplitters.containsKey(w)) {
					w = getJoiner((Splitter<?, ?>) w);
				}
				if (app.sink.equals(w))
					continue;
				Worker<?, ?> down = Workers.getSuccessors(w).get(0);
				int nextmachine = getAssignedMachine(
						Workers.getIdentifier(down), config, partition);
				for (int j = val; j < workerList.size(); j++) {
					Worker<?, ?> w1 = workerList.get(j);
					if (skippedSplitters.containsKey(w1)) {
						partition.get(nextmachine).addAll(
								skippedSplitters.get(w1));
					} else
						partition.get(nextmachine).add(w1);
				}
			}
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

	private int getAssignedMachine(int id, Configuration config,
			Map<Integer, Set<Worker<?, ?>>> partition) {
		String name = getParamName(id);
		SwitchParameter<Integer> sp = (SwitchParameter<Integer>) config
				.getParameter(name);
		if (sp == null) {
			throw new IllegalArgumentException(
					"Some parameters are missing in the passed Configuration");
		}

		int machine = sp.getValue();

		if (!partition.containsKey(machine)) {
			Set<Worker<?, ?>> set = new HashSet<>();
			partition.put(machine, set);
		}

		return machine;
	}

	private class PickHotSpots extends StreamVisitor {

		private int workerCount;

		private int paramCount;

		boolean addThis;

		private final int cutLimit = 4;

		private int depth;

		private boolean skip;

		private Configuration.Builder builder;

		private Worker<?, ?> currentHotSpot;

		private Joiner<?, ?> skipJoiner;

		private int minSplitjoinSize = 20;

		/**
		 * Workers those are going to be part {@link OfflineTuner}
		 * {@link #currentHotSpot}.
		 */
		List<Worker<?, ?>> workerGropups;

		/**
		 * This is needed for {@link SwitchParameter} as an universe argument.
		 * See its constructor.
		 */
		private final List<Integer> machinelist;

		public PickHotSpots(int noOfMachines) {
			this.machinelist = new ArrayList<>(noOfMachines);
			for (int i = 1; i <= noOfMachines; i++)
				machinelist.add(i);
			builder = Configuration.builder();
			addThis = false;
			depth = 0;
			paramCount = 0;
			skip = false;
		}

		@Override
		public void beginVisit() {
			workerCount = 0;
			depth = 0;
			paramCount = 0;
			skip = false;
			partitionGroup = new HashMap<>();
			skippedSplitters = new HashMap<>();
		}

		@Override
		public void visitFilter(Filter<?, ?> filter) {
			workerCount++;
			if (!skip)
				visitWorker(filter);
		}

		@Override
		public boolean enterPipeline(Pipeline<?, ?> pipeline) {
			return true;
		}

		@Override
		public void exitPipeline(Pipeline<?, ?> pipeline) {
		}

		@Override
		public boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
			return true;
		}

		@Override
		public void visitSplitter(Splitter<?, ?> splitter) {
			workerCount++;
			Set<Worker<?, ?>> childWorkers = new HashSet<>();
			getAllChildWorkers(splitter, childWorkers);
			if (!skip) {
				visitWorker(splitter);
				if (childWorkers.size() < minSplitjoinSize) {
					skip = true;
					skipJoiner = getJoiner(splitter);
					skippedSplitters.put(splitter, childWorkers);
				}
			}
		}

		@Override
		public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
			if (!skip)
				addThis = true;
			return true;
		}

		@Override
		public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
		}

		@Override
		public void visitJoiner(Joiner<?, ?> joiner) {
			workerCount++;
			if (skip) {
				if (skipJoiner.equals(joiner))
					skip = false;
			} else {
				addThis = true;
				visitWorker(joiner);
			}
		}

		@Override
		public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {

		}

		@Override
		public void endVisit() {
			addThis = true;
			visitFilter(null);
			System.out.println("total parameters = " + paramCount);
			System.out.println("total workers = " + (workerCount - 1));
		}

		private void visitWorker(Worker<?, ?> w) {
			assert depth <= cutLimit : "depth can not be greater than cutLimit. Verify the algorithm";

			if (currentHotSpot == null) { // Handles first visit case. Source.
				currentHotSpot = w;
				workerGropups = new ArrayList<>();
			} else if (app.sink.equals(w)) { // Handles last case. Sink.
				addThis = true;
			}
			depth++;
			if (depth > cutLimit || addThis) {
				int id = Workers.getIdentifier(currentHotSpot);
				Parameter p = new Configuration.SwitchParameter<Integer>(
						getParamName(id), Integer.class, 1, machinelist);
				builder.addParameter(p);
				if (depth > 2) {
					Parameter cut = new Configuration.IntParameter(
							String.format("worker%dcut",
									Workers.getIdentifier(currentHotSpot)), 1,
							depth - 1, depth - 1);

					builder.addParameter(cut);
					paramCount++;
				}

				currentHotSpot = w;
				partitionGroup.put(id, workerGropups);
				workerGropups = new ArrayList<>();
				workerGropups.add(currentHotSpot);
				addThis = false;
				depth = 1;
				paramCount++;
			} else {
				workerGropups.add(w);
			}
		}
	}
}
