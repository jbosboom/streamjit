package edu.mit.streamjit.impl.distributed;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamVisitor;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.Configuration.Parameter;
import edu.mit.streamjit.impl.distributed.ConfigurationManager.AbstractConfigurationManager;

public final class HotSpotTuning extends AbstractConfigurationManager {

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

		return true;
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

			if (currentHotSpot == null) // Handles first visit case.
				currentHotSpot = w;
			depth++;
			if (depth > cutLimit) {
				Parameter p = new Configuration.SwitchParameter<Integer>(
						String.format("worker%dtomachine",
								Workers.getIdentifier(currentHotSpot)),
						Integer.class, 1, machinelist);
				builder.addParameter(p);
				if (depth > 1) {
					Parameter cut = new Configuration.IntParameter(
							String.format("worker%dcut",
									Workers.getIdentifier(currentHotSpot)), 1,
							cutLimit, cutLimit);

					builder.addParameter(cut);
				}
				depth = 1;
				currentHotSpot = w;
				paramCount++;
			} else if (addThis) {
				Parameter p = new Configuration.SwitchParameter<Integer>(
						String.format("worker%dtomachine",
								Workers.getIdentifier(currentHotSpot)),
						Integer.class, 1, machinelist);
				builder.addParameter(p);
				if (depth > 2) {
					Parameter cut = new Configuration.IntParameter(
							String.format("worker%dcut",
									Workers.getIdentifier(currentHotSpot)), 1,
							depth - 1, depth - 1);

					builder.addParameter(cut);
					paramCount++;
				}
				addThis = false;
				depth = 1;
				currentHotSpot = w;
				paramCount++;
			}
		}
	}
}
