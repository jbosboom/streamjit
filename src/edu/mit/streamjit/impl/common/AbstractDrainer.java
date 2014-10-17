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
package edu.mit.streamjit.impl.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Sets;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.DrainedData;
import edu.mit.streamjit.impl.distributed.runtimer.OnlineTuner;

/**
 * Abstract drainer is to perform draining on a stream application. Both
 * {@link DistributedStreamCompiler} and {@link ConcurrentStreamCompiler} may
 * extends this to implement the draining on their particular context. Works
 * coupled with {@link BlobNode} and {@link BlobGraph}.
 * 
 * <p>
 * Three type of draining could be carried out.
 * <ol>
 * <li>Intermediate draining: In this case, no data from input buffer will be
 * consumed and StreamJit app will not be stopped. Rather, StreamJit app will be
 * just paused for reconfiguration purpose. This draining may be triggered by
 * {@link OnlineTuner}.</li>
 * <li>Semi final draining: In this case, no data from input buffer will be
 * consumed but StreamJit app will be stopped. i.e, StreamJit app will be
 * stopped safely without consuming any new input. This draining may be
 * triggered by {@link OnlineTuner} after opentuner finish tuning and send it's
 * final configuration.</li>
 * <li>Final draining: At the end of input data. After this draining StreamJit
 * app will stop. This draining may be triggered by a {@link Input} when it run
 * out of input data.</li>
 * </ol>
 * </p>
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jul 30, 2013
 */
public abstract class AbstractDrainer {

	/**
	 * This is added for debugging purpose. Just logs the size of the drain data
	 * on each channel for every draining. Calling
	 * AbstractDrainer#dumpDraindataStatistics() will write down the statistics
	 * into a file. This map and all related lines may be removed after system
	 * got stable.
	 */
	private Map<Token, List<Integer>> drainDataStatistics = null;

	/**
	 * Blob graph of the stream application that needs to be drained.
	 */
	protected BlobGraph blobGraph;

	/**
	 * Latch to block the external thread that calls
	 * {@link CompiledStream#awaitDrained()}.
	 */
	private final CountDownLatch finalLatch;

	/**
	 * Blocks the online tuner thread until drainer gets all drained data.
	 */
	private CountDownLatch drainDataLatch;

	private AtomicInteger noOfDrainData;

	/**
	 * Latch to block online tuner thread until intermediate draining is
	 * accomplished.
	 */
	private CountDownLatch intermediateLatch;

	private AtomicInteger unDrainedNodes;

	private ScheduledExecutorService schExecutorService;

	/**
	 * State of the drainer.
	 */
	private DrainerState state;

	public AbstractDrainer() {
		state = DrainerState.NODRAINING;
		finalLatch = new CountDownLatch(1);
	}

	/**
	 * Sets the blobGraph that is in execution. When
	 * {@link #startDraining(boolean)} is called, abstract drainer will traverse
	 * through the blobgraph and drain the stream application.
	 * 
	 * @param blobGraph
	 */
	public final void setBlobGraph(BlobGraph blobGraph) {
		if (state == DrainerState.NODRAINING) {
			this.blobGraph = blobGraph;
			unDrainedNodes = new AtomicInteger(blobGraph.getBlobIds().size());
			noOfDrainData = new AtomicInteger(blobGraph.getBlobIds().size());
			blobGraph.setDrainer(this);
		} else {
			throw new RuntimeException("Drainer is in draing mode.");
		}
	}

	/**
	 * Initiate the draining of the blobgraph. Three type of draining could be
	 * carried out.
	 * <ol>
	 * <li>type 0 - Intermediate draining: In this case, no data from input
	 * buffer will be consumed and StreamJit app will not be stopped. Rather,
	 * StreamJit app will be just paused for reconfiguration purpose. This
	 * draining may be triggered by {@link OnlineTuner}.</li>
	 * <li>type 1 - Semi final draining: In this case, no data from input buffer
	 * will be consumed but StreamJit app will be stopped. i.e, StreamJit app
	 * will be stopped safely without consuming any new input. This draining may
	 * be triggered by {@link OnlineTuner} after opentuner finish tuning and
	 * send it's final configuration.</li>
	 * <li>type 2 - Final draining: At the end of input data. After this
	 * draining StreamJit app will stop. This draining may be triggered by a
	 * {@link Input} when it run out of input data.</li>
	 * </ol>
	 * 
	 * @param type
	 *            whether the draining is the final draining or intermediate
	 *            draining.
	 * @return true iff draining process has been started. startDraining will
	 *         fail if the final draining has already been called.
	 */
	public final boolean startDraining(int type) {
		if (state == DrainerState.NODRAINING) {
			switch (type) {
				case 0 :
					this.blobGraph.clearDrainData();
					this.state = DrainerState.INTERMEDIATE;
					drainDataLatch = new CountDownLatch(1);
					intermediateLatch = new CountDownLatch(1);
					prepareDraining(false);
					break;
				case 1 :
					this.state = DrainerState.FINAL;
					prepareDraining(false);
					break;
				case 2 :
					this.state = DrainerState.FINAL;
					prepareDraining(true);
					break;
				default :
					throw new IllegalArgumentException(
							"Invalid draining type. type can be 0, 1, or 2.");
			}

			if (GlobalConstants.needDrainDeadlockHandler)
				this.schExecutorService = Executors
						.newSingleThreadScheduledExecutor();

			blobGraph.getSourceBlobNode().drain();

			return true;
		} else if (state == DrainerState.FINAL) {
			return false;
		} else {
			throw new RuntimeException("Drainer is in draing mode.");
		}
	}

	/**
	 * Once draining of a blob is done, it has to inform to the drainer by
	 * calling this method.
	 */
	public final void drained(Token blobID) {
		blobGraph.getBlobNode(blobID).drained();
	}

	public final void awaitDrainData() throws InterruptedException {
		drainDataLatch.await();
	}

	public final void newDrainData(DrainedData drainedData) {
		blobGraph.getBlobNode(drainedData.blobID).setDrainData(drainedData);
		if (noOfDrainData.decrementAndGet() == 0) {
			assert state == DrainerState.NODRAINING;
			drainDataLatch.countDown();
		}
	}

	// TODO: Too many unnecessary data copies are taking place at here, inside
	// the DrainData constructor and DrainData.merge(). Need to optimise these
	// all.
	/**
	 * @return Aggregated DrainData after the draining.
	 */
	public final DrainData getDrainData() {
		DrainData drainData = null;
		Map<Token, ImmutableList<Object>> boundaryInputData = new HashMap<>();
		Map<Token, ImmutableList<Object>> boundaryOutputData = new HashMap<>();

		for (BlobNode node : blobGraph.blobNodes.values()) {
			boundaryInputData.putAll(node.drainData.inputData);
			boundaryOutputData.putAll(node.drainData.outputData);
			if (drainData == null)
				drainData = node.drainData.drainData;
			else
				drainData = drainData.merge(node.drainData.drainData);
		}

		ImmutableMap.Builder<Token, ImmutableList<Object>> dataBuilder = ImmutableMap
				.builder();
		for (Token t : Sets.union(boundaryInputData.keySet(),
				boundaryOutputData.keySet())) {
			ImmutableList<Object> in = boundaryInputData.get(t) != null
					? boundaryInputData.get(t)
					: ImmutableList.of();
			ImmutableList<Object> out = boundaryOutputData.get(t) != null
					? boundaryOutputData.get(t)
					: ImmutableList.of();
			dataBuilder.put(t, ImmutableList.builder().addAll(in).addAll(out)
					.build());
		}

		ImmutableTable<Integer, String, Object> state = ImmutableTable.of();
		DrainData draindata1 = new DrainData(dataBuilder.build(), state);
		drainData = drainData.merge(draindata1);

		if (drainDataStatistics == null) {
			drainDataStatistics = new HashMap<>();
			for (Token t : drainData.getData().keySet()) {
				drainDataStatistics.put(t, new ArrayList<Integer>());
			}
		}

		for (Token t : drainData.getData().keySet()) {
			// System.out.print("Aggregated data: " + t.toString() + " - "
			// + drainData.getData().get(t).size() + " - ");
			// for (Object o : drainData.getData().get(t)) {
			// System.out.print(o.toString() + ", ");
			// }
			// System.out.print('\n');

			drainDataStatistics.get(t).add(drainData.getData().get(t).size());
		}

		return drainData;
	}

	/**
	 * logs the size of the drain data on each channel for every draining and
	 * writes down the statistics into a file.
	 * 
	 * @throws IOException
	 */
	public void dumpDraindataStatistics() throws IOException {
		if (drainDataStatistics == null) {
			System.err.println("drainDataStatistics is null");
			return;
		}

		FileWriter writer = new FileWriter("DrainDataStatistics.txt");
		for (Token t : drainDataStatistics.keySet()) {
			writer.write(t.toString());
			writer.write(" - ");
			for (Integer i : drainDataStatistics.get(t)) {
				writer.write(i.toString() + '\n');
			}
			writer.write('\n');
		}
		writer.flush();
		writer.close();
	}

	/**
	 * @return true iff draining of the stream application is finished. See
	 *         {@link CompiledStream#isDrained()} for more details.
	 */
	public final boolean isDrained() {
		return finalLatch.getCount() == 0;
	}

	/**
	 * See {@link CompiledStream#awaitDrained()} for more details.
	 */
	public final void awaitDrained() throws InterruptedException {
		finalLatch.await();
	}

	public final void awaitDrainedIntrmdiate() throws InterruptedException {
		intermediateLatch.await();

		// Just for debugging purpose. To make effect of this code snippet
		// comment the above, intermediateLatch.await(), line. Otherwise no
		// effect.
		while (intermediateLatch.getCount() != 0) {
			Thread.sleep(3000);
			System.out.println("****************************************");
			for (BlobNode bn : blobGraph.blobNodes.values()) {
				switch (bn.drainState.get()) {
					case 0 :
						System.out.println(String.format("%s - No drain call",
								bn.blobID));
						break;
					case 1 :
						System.out.println(String.format(
								"%s - Drain requested", bn.blobID));
						break;
					case 2 :
						System.out
								.println(String
										.format("%s - Dead lock detected. Artificial drained has been called",
												bn.blobID));
						break;
					case 3 :
						System.out.println(String.format(
								"%s - Drain completed", bn.blobID));
						break;
					case 4 :
						System.out.println(String.format(
								"%s - DrainData Received", bn.blobID));
						break;
				}
			}
			System.out.println("****************************************");
		}
	}

	/**
	 * In any case, if the application could not be executed (may be due to
	 * {@link Error}), {@link StreamCompiler} or appropriate class can call this
	 * method to release the main thread.
	 */
	public void stop() {
		assert state != DrainerState.INTERMEDIATE : "DrainerState.NODRAINING or DrainerState.FINAL is expected.";
		this.finalLatch.countDown();
	}

	/**
	 * See {@link CompiledStream#awaitDrained(long, TimeUnit)} for more details.
	 */
	public final void awaitDrained(long timeout, TimeUnit unit)
			throws InterruptedException, TimeoutException {
		finalLatch.await(timeout, unit);
	}

	/**
	 * Once a {@link BlobNode}'s all preconditions are satisfied for draining,
	 * blob node will call this function drain the blob.
	 * 
	 * @param blobID
	 * @param isFinal
	 *            : whether the draining is the final draining or intermediate
	 *            draining. Set to true for semi final case.
	 */
	protected abstract void drain(Token blobID, boolean isFinal);

	/**
	 * {@link AbstractDrainer} will call this function after the corresponding
	 * blob is drained. Sub classes may implement blob related resource cleanup
	 * jobs here ( e.g., stop blob threads).
	 * 
	 * @param blobID
	 * @param isFinal
	 *            : whether the draining is the final draining or intermediate
	 *            draining. Set to true for semi final case.
	 */
	protected abstract void drainingDone(Token blobID, boolean isFinal);

	/**
	 * {@link AbstractDrainer} will call this function after the draining
	 * process is complete. This can be used to do the final cleanups ( e.g, All
	 * data in the tail buffer should be consumed before this function returns.)
	 * After the return of this function, isDrained() will start to return true
	 * and any threads waiting at awaitdraining() will be released.
	 * 
	 * @param isFinal
	 *            : whether the draining is the final draining or intermediate
	 *            draining. Set to true for semi final case.
	 */
	protected abstract void drainingDone(boolean isFinal);

	/**
	 * {@link AbstractDrainer} will call this function as a first step to start
	 * a draining.
	 * 
	 * @param isFinal
	 *            :Whether the draining is the final draining or intermediate
	 *            draining. Set to false for semi final case.
	 */
	protected abstract void prepareDraining(boolean isFinal);

	/**
	 * {@link BlobNode}s have to call this function to inform draining done
	 * event.
	 * 
	 * @param blobNode
	 */
	private void drainingDone(BlobNode blobNode) {
		assert state != DrainerState.NODRAINING : "Illegal call. Drainer is not in draining mode.";
		drainingDone(blobNode.blobID, state == DrainerState.FINAL);
		if (unDrainedNodes.decrementAndGet() == 0) {
			drainingDone(state == DrainerState.FINAL);
			if (state == DrainerState.FINAL) {
				finalLatch.countDown();
			} else {
				state = DrainerState.NODRAINING;
				intermediateLatch.countDown();
			}

			if (GlobalConstants.needDrainDeadlockHandler)
				schExecutorService.shutdownNow();
		}
	}

	/**
	 * BlobGraph builds predecessor successor relationship for set of
	 * partitioned workers, and verifies for cyclic dependencies among the
	 * partitions. Blob graph doesn't keep blobs. Instead it keeps
	 * {@link BlobNode} that represents blobs. </p> All BlobNodes in the graph
	 * can be retrieved and used in coupled with {@link AbstractDrainer} to
	 * successfully perform draining process.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Jul 30, 2013
	 */
	public static class BlobGraph {

		/**
		 * All nodes in the graph.
		 */
		private final ImmutableMap<Token, BlobNode> blobNodes;

		/**
		 * The blob which has the overall stream input.
		 */
		private final BlobNode sourceBlobNode;

		public BlobGraph(List<Set<Worker<?, ?>>> partitionWorkers) {
			checkNotNull(partitionWorkers);
			Set<DummyBlob> blobSet = new HashSet<>();
			for (Set<Worker<?, ?>> workers : partitionWorkers) {
				blobSet.add(new DummyBlob(workers));
			}

			ImmutableMap.Builder<Token, BlobNode> builder = new ImmutableMap.Builder<>();
			for (DummyBlob b : blobSet) {
				builder.put(b.id, new BlobNode(b.id));
			}

			this.blobNodes = builder.build();

			for (DummyBlob cur : blobSet) {
				for (DummyBlob other : blobSet) {
					if (cur == other)
						continue;
					if (Sets.intersection(cur.outputs, other.inputs).size() != 0) {
						BlobNode curNode = blobNodes.get(cur.id);
						BlobNode otherNode = blobNodes.get(other.id);

						curNode.addSuccessor(otherNode);
						otherNode.addPredecessor(curNode);
					}
				}
			}

			checkCycles(blobNodes.values());

			BlobNode sourceBlob = null;
			for (BlobNode bn : blobNodes.values()) {
				if (bn.getDependencyCount() == 0) {
					assert sourceBlob == null : "Multiple independent blobs found.";
					sourceBlob = bn;
				}
			}

			checkNotNull(sourceBlob);
			this.sourceBlobNode = sourceBlob;
		}

		/**
		 * @return BlobIds of all blobnodes in the blobgraph.
		 */
		public ImmutableSet<Token> getBlobIds() {
			return blobNodes.keySet();
		}

		public BlobNode getBlobNode(Token blobID) {
			return blobNodes.get(blobID);
		}

		/**
		 * A Drainer can be set to the {@link BlobGraph} to perform draining.
		 * 
		 * @param drainer
		 */
		public void setDrainer(AbstractDrainer drainer) {
			for (BlobNode bn : blobNodes.values()) {
				bn.setDrainer(drainer);
			}
		}

		public void clearDrainData() {
			for (BlobNode node : blobNodes.values()) {
				node.drainData = null;
			}
		}

		/**
		 * @return the sourceBlobNode
		 */
		private BlobNode getSourceBlobNode() {
			return sourceBlobNode;
		}

		/**
		 * Does a depth first traversal to detect cycles in the graph.
		 * 
		 * @param blobNodes
		 */
		private void checkCycles(Collection<BlobNode> blobNodes) {
			Map<BlobNode, Color> colorMap = new HashMap<>();
			for (BlobNode b : blobNodes) {
				colorMap.put(b, Color.WHITE);
			}
			for (BlobNode b : blobNodes) {
				if (colorMap.get(b) == Color.WHITE)
					if (DFS(b, colorMap))
						throw new StreamCompilationFailedException(
								"Cycles found among blobs");
			}
		}

		/**
		 * A cycle exits in a directed graph if a back edge is detected during a
		 * DFS traversal. A back edge exists in a directed graph if the
		 * currently explored vertex has an adjacent vertex that was already
		 * colored gray
		 * 
		 * @param vertex
		 * @param colorMap
		 * @return <code>true</code> if cycle found, <code>false</code>
		 *         otherwise.
		 */
		private boolean DFS(BlobNode vertex, Map<BlobNode, Color> colorMap) {
			colorMap.put(vertex, Color.GRAY);
			for (BlobNode adj : vertex.getSuccessors()) {
				if (colorMap.get(adj) == Color.GRAY)
					return true;
				if (colorMap.get(adj) == Color.WHITE)
					if (DFS(adj, colorMap))
						return true;
			}
			colorMap.put(vertex, Color.BLACK);
			return false;
		}

		/**
		 * Just used to build the input and output tokens of a partitioned blob
		 * workers. imitate a {@link Blob}.
		 */
		private final class DummyBlob {
			private final ImmutableSet<Token> inputs;
			private final ImmutableSet<Token> outputs;
			private final Token id;

			private DummyBlob(Set<Worker<?, ?>> workers) {
				ImmutableSet.Builder<Token> inputBuilder = new ImmutableSet.Builder<>();
				ImmutableSet.Builder<Token> outputBuilder = new ImmutableSet.Builder<>();
				for (IOInfo info : IOInfo.externalEdges(workers)) {
					(info.isInput() ? inputBuilder : outputBuilder).add(info
							.token());
				}

				inputs = inputBuilder.build();
				outputs = outputBuilder.build();
				id = Collections.min(inputs);
			}
		}
	}

	/**
	 * BlobNode represents the vertex in the blob graph ({@link BlobGraph}). It
	 * represents a {@link Blob} and carry the draining process of that blob.
	 * 
	 * @author Sumanan
	 */
	private static final class BlobNode {

		/**
		 * Intermediate drain data.
		 */
		private DrainedData drainData;

		private AbstractDrainer drainer;
		/**
		 * The blob that wrapped by this blob node.
		 */
		private final Token blobID;
		/**
		 * Predecessor blob nodes of this blob node.
		 */
		private List<BlobNode> predecessors;
		/**
		 * Successor blob nodes of this blob node.
		 */
		private List<BlobNode> successors;
		/**
		 * The number of undrained predecessors of this blobs. Everytime, when a
		 * predecessor finished draining, dependencyCount will be decremented
		 * and once it reached to 0 this blob will be called for draining.
		 */
		private AtomicInteger dependencyCount;

		// TODO: add comments
		private AtomicInteger drainState;

		private BlobNode(Token blob) {
			this.blobID = blob;
			predecessors = new ArrayList<>();
			successors = new ArrayList<>();
			dependencyCount = new AtomicInteger(0);
			drainState = new AtomicInteger(0);
		}

		/**
		 * Should be called when the draining of the current blob has been
		 * finished. This function stops all threads belong to the blob and
		 * inform its successors as well.
		 */
		private void drained() {
			if (drainState.compareAndSet(1, 3)) {
				for (BlobNode suc : this.successors) {
					suc.predecessorDrained(this);
				}
				drainer.drainingDone(this);
			} else if (drainState.compareAndSet(2, 3)) {
				drainer.drainingDone(this);
			}
		}

		/**
		 * Drain the blob mapped by this blob node.
		 */
		private void drain() {
			checkNotNull(drainer);
			if (!drainState.compareAndSet(0, 1)) {
				throw new IllegalStateException(
						"Drain of this blobNode has already been called");
			}
			drainer.drain(blobID, drainer.state == DrainerState.FINAL);

			// TODO: Verify the waiting time is reasonable.
			if (GlobalConstants.needDrainDeadlockHandler)
				drainer.schExecutorService.schedule(deadLockHandler(), 6000,
						TimeUnit.MILLISECONDS);
		}

		private void setDrainData(DrainedData drainedData) {
			if (this.drainData == null) {
				this.drainData = drainedData;
				drainState.set(4);
			} else
				throw new AssertionError(
						"Multiple drain data has been received.");
		}

		private ImmutableList<BlobNode> getSuccessors() {
			return ImmutableList.copyOf(successors);
		}

		private void addPredecessor(BlobNode pred) {
			assert !predecessors.contains(pred) : String.format(
					"The BlobNode %s has already been set as a predecessors",
					pred);
			predecessors.add(pred);
			dependencyCount.set(dependencyCount.get() + 1);
		}

		private void addSuccessor(BlobNode succ) {
			assert !successors.contains(succ) : String
					.format("The BlobNode %s has already been set as a successor",
							succ);
			successors.add(succ);
		}

		private void predecessorDrained(BlobNode pred) {
			if (!predecessors.contains(pred))
				throw new IllegalArgumentException("Illegal Predecessor");

			assert dependencyCount.get() > 0 : String
					.format("Graph mismatch : My predecessors count is %d. But more than %d of BlobNodes claim me as their successor",
							predecessors.size(), predecessors.size());

			if (dependencyCount.decrementAndGet() == 0) {
				drain();
			}
		}

		/**
		 * @return The number of undrained predecessors.
		 */
		private int getDependencyCount() {
			return dependencyCount.get();
		}

		private void setDrainer(AbstractDrainer drainer) {
			checkNotNull(drainer);
			this.drainer = drainer;
		}

		private Runnable deadLockHandler() {
			Runnable r = new Runnable() {

				@Override
				public void run() {
					if (drainState.compareAndSet(1, 2)) {
						for (BlobNode suc : successors) {
							suc.predecessorDrained(BlobNode.this);
						}
						System.out
								.println("deadLockHandler: "
										+ blobID
										+ " - Deadlock during draining has been handled");
					}
				}
			};
			return r;
		}
	}

	/**
	 * Color enumerator used by DFS algorithm to find cycles in the blob graph.
	 */
	private enum Color {
		WHITE, GRAY, BLACK
	}

	/**
	 * Reflects {@link AbstractDrainer}'s state.
	 */
	private enum DrainerState {
		NODRAINING, /**
		 * Draining in middle of the stream graph's execution. This
		 * type of draining will be triggered by the open tuner for
		 * reconfiguration. Drained data of all blobs are expected in this case.
		 */
		INTERMEDIATE, /**
		 * This type of draining will take place when input stream
		 * runs out. No drained data expected as all blob are expected to
		 * executes until all input buffers become empty.
		 */
		FINAL
	}
}
