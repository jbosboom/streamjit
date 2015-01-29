package edu.mit.streamjit.impl.distributed;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
import edu.mit.streamjit.api.StreamVisitor;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.util.ConfigurationUtils;

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
	 * Generates dot file and then from the dot file generates graph. Before
	 * using this class, ensure that Graphviz is properly installed in the
	 * system.
	 */
	public static class DotVisualizer implements Visualizer {

		protected final OneToOneElement<?, ?> streamGraph;

		private final String appName;

		/**
		 * namePrefix of the current configuration.
		 */
		private String namePrefix = "";

		/**
		 * Tells whether the dot tool is installed in the system or not.
		 */
		private boolean hasDot;

		public DotVisualizer(OneToOneElement<?, ?> streamGraph) {
			this.streamGraph = streamGraph;
			this.appName = streamGraph.getClass().getSimpleName();
			hasDot = true;
			DOTstreamVisitor dotSV = new DOTstreamVisitor();
			streamGraph.visit(dotSV);
		}

		/**
		 * Visits through the Stream graph and generates dot file.
		 * 
		 * @author sumanan
		 * @since 29 Dec, 2014
		 */
		private class DOTstreamVisitor extends StreamVisitor {

			private final FileWriter writer;

			DOTstreamVisitor() {
				writer = Utils.fileWriter(String.format("%s%sstreamgraph.dot",
						appName, File.separator));
			}

			private void initilizeDot() {
				try {
					writer.write(String.format("digraph %s {\n", appName));
					writer.write("\trankdir=TD;\n");
					writer.write("\tnodesep=0.5;\n");
					writer.write("\tranksep=equally;\n");
					// writer.write("\tnode [shape = circle];\n");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			private void closeDot() {
				try {
					writer.write("}");
					writer.flush();
					writer.close();
				} catch (IOException e) {
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
				runDot("streamgraph");
			}

			private void updateDot(Worker<?, ?> w) {
				for (Worker<?, ?> suc : Workers.getSuccessors(w)) {
					String first = w.getClass().getSimpleName();
					String second = suc.getClass().getSimpleName();
					int id = Workers.getIdentifier(w);
					int sucID = Workers.getIdentifier(suc);
					try {
						writer.write(String.format("\t%d -> %d;\n", id, sucID));
						// writer.write(String.format("\t%s -> %s;\n", first,
						// second));
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

		@Override
		public void newConfiguration(Configuration cfg) {
			namePrefix = ConfigurationUtils.getConfigPrefix(cfg);
		}

		private void runDot(String file) {
			String fileName = String.format("./%s%s%s.dot", appName,
					File.separator, file);
			String outFileName = String.format(
					"./%s%sconfigurations%s%s_%s.png", appName, File.separator,
					File.separator, namePrefix, file);
			ProcessBuilder pb = new ProcessBuilder("dot", "-Tpng", fileName,
					"-o", outFileName);
			try {
				Process p = pb.start();
				p.waitFor();
			} catch (IOException | InterruptedException e) {
				System.err
						.println("DotVisualizer: dot(Graphviz) tool is not properly installed in the system");
				hasDot = false;
				// e.printStackTrace();
			}
		}

		@Override
		public void newPartitionMachineMap(
				Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap) {
			if (!hasDot)
				return;
			FileWriter writer;
			try {
				writer = blobGraphWriter();
				for (int machine : partitionsMachineMap.keySet()) {
					for (Set<Worker<?, ?>> blobworkers : partitionsMachineMap
							.get(machine)) {
						Token blobID = Utils.getblobID(blobworkers);
						writer.write(String
								.format("\tsubgraph \"cluster_%s\" { color="
										+ "royalblue1; label = \"Blob-%s:Machine-%d\";",
										blobID, blobID, machine));
						Set<Integer> workerIDs = getWorkerIds(blobworkers);
						for (Integer id : workerIDs)
							writer.write(String.format(" %d;", id));
						writer.write("}\n");
					}
				}
				writer.write("}\n");
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			runDot("blobgraph");
		}
		private Set<Integer> getWorkerIds(Set<Worker<?, ?>> blobworkers) {
			Set<Integer> workerIds = new HashSet<>();
			for (Worker<?, ?> w : blobworkers) {
				workerIds.add(Workers.getIdentifier(w));
			}
			return workerIds;
		}

		/**
		 * Copies all lines except the final closing bracket from
		 * streamgraph.dot to blobgraph.dot.
		 * 
		 * @return
		 * @throws IOException
		 */
		private FileWriter blobGraphWriter() throws IOException {
			File streamGraph = new File(String.format("./%s%sstreamgraph.dot",
					appName, File.separator));
			File blobGraph = new File(String.format("./%s%sblobgraph.dot",
					appName, File.separator));
			BufferedReader reader = new BufferedReader(new FileReader(
					streamGraph));
			FileWriter writer = new FileWriter(blobGraph, false);
			String line;
			int unclosedParenthesis = 0;
			while ((line = reader.readLine()) != null) {
				if (line.contains("{"))
					unclosedParenthesis++;
				if (line.contains("}"))
					unclosedParenthesis--;
				if (unclosedParenthesis > 0) {
					writer.write(line);
					writer.write("\n");
				}
			}
			reader.close();
			return writer;
		}
	}
}
