package edu.mit.streamjit.impl.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.mit.streamjit.impl.blob.Blob;

/**
 * BlobGraph builds predecessor successor relationship for set of {@link Blob}s
 * and verifies for cyclic dependencies among the blobs. Further, it gives
 * {@link Drainer} that can be get used perform draining over the blob graph.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jul 28, 2013
 */
public final class BlobGraph {

	private final Drainer drainer;

	private final ImmutableSet<Blob> blobSet;

	public BlobGraph(Set<Blob> blobSet) {
		this.blobSet = ImmutableSet.copyOf(blobSet);
		Set<BlobNode> blobNodes = new HashSet<>(blobSet.size());
		for (Blob b : blobSet) {
			blobNodes.add(new BlobNode(b));
		}

		for (BlobNode cur : blobNodes) {
			for (BlobNode other : blobNodes) {
				if (cur == other)
					continue;
				if (Sets.intersection(cur.getBlob().getOutputs(),
						other.getBlob().getInputs()).size() != 0) {
					cur.addSuccessor(other);
					other.addPredecessor(cur);
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

		drainer = Drainer.getDrainer(blobNodes, sourceBlob);
	}

	/**
	 * @return The drainer object of this Blobgraph that can be used for
	 *         draining.
	 */
	public Drainer getDrainer() {
		return drainer;
	}

	/**
	 * Does a depth first traversal to detect cycles in the graph.
	 * 
	 * @param blobNodes
	 */
	private void checkCycles(Set<BlobNode> blobNodes) {
		Map<BlobNode, Color> colorMap = new HashMap<>();
		for (BlobNode b : blobNodes) {
			colorMap.put(b, Color.WHITE);
		}
		for (BlobNode b : blobNodes) {
			if (colorMap.get(b) == Color.WHITE)
				if (DFS(b, colorMap))
					throw new AssertionError("Cycles found among blobs");
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
				DFS(adj, colorMap);
		}
		colorMap.put(vertex, Color.BLACK);
		return false;
	}

	/**
	 * @return Set of blobs in the blob graph.
	 */
	public ImmutableSet<Blob> getBlobSet() {
		return blobSet;
	}

	/**
	 * Drainer triggers the draining operation for a blob graph. Once draining
	 * is started, blobs will be called for draining iff all of their
	 * predecessor blobs have been drained.
	 */
	public static class Drainer {

		/**
		 * All nodes in the blob graph.
		 */
		private final Set<BlobNode> blobNodes;

		/**
		 * The blob that has overall input token.
		 */
		private final BlobNode sourceBlob;

		private static Drainer getDrainer(Set<BlobNode> blobNodes,
				BlobNode sourceBlob) {
			return new Drainer(blobNodes, sourceBlob);
		}

		private Drainer(Set<BlobNode> blobNodes, BlobNode sourceBlob) {
			this.sourceBlob = sourceBlob;
			this.blobNodes = blobNodes;
		}

		/**
		 * @param threadMap
		 *            Map that contains blobs and corresponding set of blob
		 *            threads belong to the blob.
		 */
		public void startDraining(Map<Blob, Set<BlobThread>> threadMap) {
			for (BlobNode bn : blobNodes) {
				if (!threadMap.containsKey(bn.getBlob()))
					throw new AssertionError(
							"threadMap doesn't contain thread information for the blob "
									+ bn.getBlob());

				bn.setBlobThreads(threadMap.get(bn.getBlob()));
			}
			sourceBlob.drain();
		}

		/**
		 * @return <code>true</code> iff all blobs in the blob graph has
		 *         finished the draining.
		 */
		public boolean isDrained() {
			for (BlobNode bn : blobNodes)
				if (!bn.isDrained())
					return false;
			return true;
		}
	}

	/**
	 * BlobNode represents the vertex in the blob graph ({@link BlobGraph}). It
	 * wraps a {@link Blob} and carry the draining process of that blob.
	 * 
	 * @author Sumanan
	 */
	private class BlobNode {
		/**
		 * The blob that wrapped by this blob node.
		 */
		private Blob blob;
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
		 * Set of threads those belong to the blob.
		 */
		private Set<BlobThread> blobThreads;

		/**
		 * Set to ture iff this blob has been drained.
		 */
		private volatile boolean isDrained;

		private BlobNode(Blob blob) {
			this.blob = blob;
			predecessors = new ArrayList<>();
			successors = new ArrayList<>();
			dependencyCount = new AtomicInteger(0);
			isDrained = false;
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

		private Blob getBlob() {
			return blob;
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

		/**
		 * Should be called when the draining of the current blob has been
		 * finished. This function stops all threads belong to the blob and
		 * inform its successors as well.
		 */
		private void drained() {
			for (BlobThread bt : blobThreads)
				bt.requestStop();

			for (BlobNode suc : this.successors) {
				suc.predecessorDrained(this);
			}

			isDrained = true;
		}

		private void drain() {
			blob.drain(new DrainCallback(this));
		}

		private void setBlobThreads(Set<BlobThread> blobThreads) {
			this.blobThreads = blobThreads;
		}

		private boolean isDrained() {
			return isDrained;
		}
	}

	private class DrainCallback implements Runnable {
		private final BlobNode myNode;

		private DrainCallback(BlobNode blobNode) {
			this.myNode = blobNode;
		}

		@Override
		public void run() {
			myNode.drained();
		}
	}

	/**
	 * Color enumerator used by DFS algorithm to find cycles in the blob graph.
	 */
	private enum Color {
		WHITE, GRAY, BLACK
	}
}
