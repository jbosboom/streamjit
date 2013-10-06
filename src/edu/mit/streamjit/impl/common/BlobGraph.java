package edu.mit.streamjit.impl.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;

/**
 * BlobGraph builds predecessor successor relationship for set of partitioned
 * workers, and verifies for cyclic dependencies among the partitions. Blob
 * graph doesn't keep blobs. Instead it keeps {@link BlobNode} that represents
 * blobs. </p> All BlobNodes in the graph can be retrieved and used in coupled
 * with {@link AbstractDrainer} to successfully perform draining process.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jul 30, 2013
 */
public class BlobGraph {

	/**
	 * All nodes in the graph.
	 */
	private final ImmutableSet<BlobNode> blobNodes;

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

		ImmutableSet.Builder<BlobNode> builder = new ImmutableSet.Builder<>();
		for (DummyBlob b : blobSet) {
			builder.add(new BlobNode(b.id));
		}

		this.blobNodes = builder.build();

		Map<Token, BlobNode> blobNodeMap = new HashMap<>();
		for (BlobNode node : blobNodes) {
			blobNodeMap.put(node.blobID, node);
		}
		for (DummyBlob cur : blobSet) {
			for (DummyBlob other : blobSet) {
				if (cur == other)
					continue;
				if (Sets.intersection(cur.outputs, other.inputs).size() != 0) {
					BlobNode curNode = blobNodeMap.get(cur.id);
					BlobNode otherNode = blobNodeMap.get(other.id);

					curNode.addSuccessor(otherNode);
					otherNode.addPredecessor(curNode);
				}
			}
		}

		checkCycles(blobNodes);

		BlobNode sourceBlob = null;
		for (BlobNode bn : blobNodes) {
			if (bn.getDependencyCount() == 0) {
				assert sourceBlob == null : "Multiple independent blobs found.";
				sourceBlob = bn;
			}
		}

		checkNotNull(sourceBlob);
		this.sourceBlobNode = sourceBlob;
	}

	/**
	 * .
	 * 
	 * @return All nodes in the graph.
	 */
	public ImmutableSet<BlobNode> getBlobNodes() {
		return blobNodes;
	}

	public BlobNode getBlobNode(Token blobID) {
		for (BlobNode bn : blobNodes) {
			if (bn.getBlobID().equals(blobID))
				return bn;
		}
		return null;
	}

	/**
	 * A Drainer can be set to the {@link BlobGraph} to perform draining.
	 * 
	 * @param drainer
	 */
	public void setDrainer(AbstractDrainer drainer) {
		for (BlobNode bn : blobNodes) {
			bn.setDrainer(drainer);
		}
	}

	/**
	 * TODO: Ensure whether providing this public method is useful.
	 * 
	 * @return the sourceBlobNode
	 */
	public BlobNode getSourceBlobNode() {
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
	 * A cycle exits in a directed graph if a back edge is detected during a DFS
	 * traversal. A back edge exists in a directed graph if the currently
	 * explored vertex has an adjacent vertex that was already colored gray
	 * 
	 * @param vertex
	 * @param colorMap
	 * @return <code>true</code> if cycle found, <code>false</code> otherwise.
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
	 * BlobNode represents the vertex in the blob graph ({@link BlobGraph}). It
	 * represents a {@link Blob} and carry the draining process of that blob.
	 * 
	 * @author Sumanan
	 */
	public static final class BlobNode {
		private AbstractDrainer drainer;
		/**
		 * The blob that wrapped by this blob node.
		 */
		private Token blobID;
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

		/**
		 * Set to true iff this blob has been drained.
		 */
		private volatile boolean isDrained;

		private BlobNode(Token blob) {
			this.blobID = blob;
			predecessors = new ArrayList<>();
			successors = new ArrayList<>();
			dependencyCount = new AtomicInteger(0);
			isDrained = false;
		}

		/**
		 * Should be called when the draining of the current blob has been
		 * finished. This function stops all threads belong to the blob and
		 * inform its successors as well.
		 */
		public void drained() {
			isDrained = true;
			for (BlobNode suc : this.successors) {
				suc.predecessorDrained(this);
			}
			drainer.drainingFinished(this);
		}

		/**
		 * Drain the blob mapped by this blob node.
		 */
		private void drain() {
			checkNotNull(drainer);
			drainer.drain(this);
		}

		/**
		 * @return <code>true</code> iff the blob mapped by this blob node was
		 *         drained.
		 */
		public boolean isDrained() {
			return isDrained;
		}

		/**
		 * @return Identifier of {@link Blob} and blob node.
		 */
		public Token getBlobID() {
			return blobID;
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

	}

	/**
	 * Abstract drainer is to perform draining on a stream application. Both
	 * {@link DistributedStreamCompiler} and {@link ConcurrentStreamCompiler}
	 * may extends this to implement the draining on their particular context.
	 * Works coupled with {@link BlobNode} and {@link BlobGraph}.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Jul 30, 2013
	 */
	public static abstract class AbstractDrainer {
		/**
		 * Blob graph of the stream application that needs to be drained.
		 */
		protected final BlobGraph blobGraph;

		private final CountDownLatch latch;

		private AtomicInteger unDrainedNodes;

		/**
		 * Whether the {@link StreamCompiler} needs the drain data after
		 * draining.
		 */
		protected boolean needDrainData;

		public AbstractDrainer(BlobGraph blobGraph, boolean needDrainData) {
			this.blobGraph = blobGraph;
			this.needDrainData = needDrainData;
			unDrainedNodes = new AtomicInteger(blobGraph.getBlobNodes().size());
			latch = new CountDownLatch(1);
			blobGraph.setDrainer(this);
		}

		public void drainingFinished(BlobNode blobNode) {
			drained(blobNode);
			if (unDrainedNodes.decrementAndGet() == 0) {
				drainingFinished();
				latch.countDown();
			}
		}

		/**
		 * Initiate the draining of the blobgraph.
		 */
		public final void startDraining(boolean isFinal) {
			blobGraph.getSourceBlobNode().drain();
		}

		/**
		 * @return true iff draining of the stream application is finished.
		 */
		public final boolean isDrained() {
			return latch.getCount() == 0;
		}

		public final void awaitDrained() throws InterruptedException {
			latch.await();
		}

		public final void awaitDrained(long timeout, TimeUnit unit)
				throws InterruptedException, TimeoutException {
			latch.await(timeout, unit);
		}

		/**
		 * Once a {@link BlobNode}'s all preconditions are satisfied for
		 * draining, blob node will call this function drain the blob.
		 * 
		 * @param node
		 */
		protected abstract void drain(BlobNode node);

		/**
		 * A blob thread ( Only one blob thread, if there are many threads on
		 * the blob) must call this function through a callback once draining of
		 * that particular blob is finished.
		 * 
		 * @param node
		 */
		protected abstract void drained(BlobNode node);

		/**
		 * Once all {@link BlobNode} have been drained, this function will get
		 * called. This can be used to do the final cleanups ( e.g, All data in
		 * the tail buffer should be consumed before this function returns.)
		 * After the return of this function, isDrained() will start to return
		 * true.
		 */
		protected abstract void drainingFinished();

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

	/**
	 * Color enumerator used by DFS algorithm to find cycles in the blob graph.
	 */
	private enum Color {
		WHITE, GRAY, BLACK
	}
}
