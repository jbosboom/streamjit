package edu.mit.streamjit.impl.distributed;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
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
import edu.mit.streamjit.impl.common.Workers;

/**
 * Interface to visualize a stream graph and it's configurations. Use the
 * constructor to get the stream graph.
 * 
 * @author Sumanan
 * @since 29 Dec, 2014
 */
public interface Visualizer {

	/**
	 * Call this method with new configuration, whenever the configuration
	 * changes.
	 * 
	 * @param cfg
	 */
	public void newConfiguration(Configuration cfg);

	/**
	 * Partitions Machine Map of the current configuration. Only the
	 * {@link ConfigurationManager} has the information to generate this map.
	 * Visualizer has no glue to generate this partitionsMachineMap.
	 * 
	 * @param partitionsMachineMap
	 */
	public void newPartitionMachineMap(
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap);

	/**
	 * Use this class to have no visualization.
	 * 
	 */
	public static class NoVisualizer implements Visualizer {

		@Override
		public void newConfiguration(Configuration cfg) {
			return;
		}

		@Override
		public void newPartitionMachineMap(
				Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap) {
			return;
		}
	}

	/**
	 * Generates dot file and then from the dot file generates graph.
	 */
	public static class DotVisualizer implements Visualizer {

		protected final OneToOneElement<?, ?> streamGraph;

		public DotVisualizer(OneToOneElement<?, ?> streamGraph) {
			this.streamGraph = streamGraph;
			String name = streamGraph.getClass().getSimpleName();
			DOTstreamVisitor dotSV = new DOTstreamVisitor(name);
			streamGraph.visit(dotSV);
		}

		/**
		 * Visits through the Stream graph and generates dot file.
		 * 
		 * @author sumanan
		 * @since 29 Dec, 2014
		 */
		private class DOTstreamVisitor extends StreamVisitor {

			private final String streamJitAppname;
			private final FileWriter writter;

			DOTstreamVisitor(String streamJitAppname) {
				this.streamJitAppname = streamJitAppname;
				writter = fileWriter();
			}

			private FileWriter fileWriter() {
				FileWriter w = null;
				String fileName = String.format("%s%sgraph.dot",
						streamJitAppname, File.separator);
				try {
					w = new FileWriter(fileName, false);
				} catch (IOException e) {
					e.printStackTrace();
				}
				return w;
			}

			private void initilizeDot() {
				try {
					writter.write(String.format("digraph %s {\n",
							streamJitAppname));
					writter.write("\trankdir=TD;\n");
					writter.write("\tnodesep=0.5;\n");
					writter.write("\tranksep=equally;\n");
					// writter.write("\tnode [shape = circle];\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			private void closeDot() {
				try {
					writter.write("}");
					writter.flush();
					writter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			private void runDot() {
				String fileName = String.format("./%s%sgraph.dot",
						streamJitAppname, File.separator);
				String outFileName = String.format("./%s%sgraph.png",
						streamJitAppname, File.separator);
				ProcessBuilder pb = new ProcessBuilder("dot", "-Tpng",
						fileName, "-o", outFileName);
				try {
					Process p = pb.start();
					p.waitFor();
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
			@Override
			public void beginVisit() {
				initilizeDot();
			}

			@Override
			public void visitFilter(Filter<?, ?> filter) {
				updateDot(filter);
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
				updateDot(splitter);
			}

			@Override
			public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
				return true;
			}

			@Override
			public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
			}

			@Override
			public void visitJoiner(Joiner<?, ?> joiner) {
				updateDot(joiner);
			}

			@Override
			public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
			}

			@Override
			public void endVisit() {
				closeDot();
				runDot();
			}

			private void updateDot(Worker<?, ?> w) {
				for (Worker<?, ?> suc : Workers.getSuccessors(w)) {
					String first = w.getClass().getSimpleName();
					String second = suc.getClass().getSimpleName();
					int id = Workers.getIdentifier(w);
					int sucID = Workers.getIdentifier(suc);
					try {
						writter.write(String.format("\t%d -> %d;\n", id, sucID));
						// writter.write(String.format("\t%s -> %s;\n", first,
						// second));
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

		@Override
		public void newConfiguration(Configuration cfg) {

		}

		@Override
		public void newPartitionMachineMap(
				Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap) {
		}
	}
}
