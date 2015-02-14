package edu.mit.streamjit.impl.common.drainer;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.IOInfo;
import edu.mit.streamjit.impl.common.drainer.AbstractDrainer.DrainerState;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainType;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainedData;

/**
 * [14 Feb, 2015] This class was an inner class of {@link AbstractDrainer}. I
 * have re factored {@link AbstractDrainer} and moved this class a new file.
 * 
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
	final ImmutableMap<Token, BlobNode> blobNodes;

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
			builder.put(b.id, new BlobNode(b.id, b.inputs, b.outputs));
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
	 * TODO: We may need to make the class {@link BlobNode} public and move
	 * these functions to {@link BlobNode}.
	 * <p>
	 * Returns output edges of a blob. This method is added on [2014-03-01].
	 * 
	 * @param blobID
	 * @return
	 */
	public ImmutableSet<Token> getOutputs(Token blobID) {
		return blobNodes.get(blobID).outputs;
	}

	/**
	 * TODO: We may need to make the class {@link BlobNode} public and move
	 * these functions to {@link BlobNode}.
	 * <p>
	 * Returns input edges of a blob. This method is added on [2014-03-01].
	 * 
	 * @param blobID
	 * @return
	 */
	public ImmutableSet<Token> getInputs(Token blobID) {
		return blobNodes.get(blobID).inputs;
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
			node.snDrainData = null;
		}
	}

	/**
	 * @return the sourceBlobNode
	 */
	BlobNode getSourceBlobNode() {
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
	 * [14 Feb, 2015] This class was an inner class of {@link AbstractDrainer}.
	 * I have re factored {@link AbstractDrainer} and moved this class to here.
	 * {@link AbstractDrainer} directly accessed lots of fields and methods of
	 * this class when this was an inner class of it. So those fields and
	 * methods of this class have been made as package private when re
	 * factoring. </p>
	 * <p>
	 * [14 Feb, 2015] TODO: {@link AbstractDrainer#schExecutorService} and
	 * {@link AbstractDrainer#state} have been made package private during the
	 * re factoring. We can make those fields private by moving
	 * {@link BlobNode#drain()} and {@link BlobNode#drained()} to
	 * {@link AbstractDrainer}.
	 * </p>
	 * 
	 * BlobNode represents the vertex in the blob graph ({@link BlobGraph} ). It
	 * represents a {@link Blob} and carry the draining process of that blob.
	 * 
	 * @author Sumanan
	 */
	static final class BlobNode {

		/**
		 * Intermediate drain data.
		 */
		SNDrainedData snDrainData;

		private AbstractDrainer drainer;
		/**
		 * The blob that wrapped by this blob node.
		 */
		final Token blobID;
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
		AtomicInteger drainState;

		/**
		 * All input channels of this blob. We need this information to globally
		 * determine buffer sizes to avoid deadlocks. This is added on
		 * [2014-03-01], when implementing global buffer size adjustment.
		 */
		private final ImmutableSet<Token> inputs;

		/**
		 * All output channels of this blob. We need this information to
		 * globally determine buffer sizes to avoid deadlocks. This is added on
		 * [2014-03-01], when implementing global buffer size adjustment.
		 */
		private final ImmutableSet<Token> outputs;

		private BlobNode(Token blob, ImmutableSet<Token> inputs,
				ImmutableSet<Token> outputs) {
			this.blobID = blob;
			predecessors = new ArrayList<>();
			successors = new ArrayList<>();
			dependencyCount = new AtomicInteger(0);
			drainState = new AtomicInteger(0);
			this.inputs = inputs;
			this.outputs = outputs;
		}

		/**
		 * Should be called when the draining of the current blob has been
		 * finished. This function stops all threads belong to the blob and
		 * inform its successors as well.
		 */
		void drained() {
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
		void drain() {
			checkNotNull(drainer);
			if (!drainState.compareAndSet(0, 1)) {
				throw new IllegalStateException(
						"Drain of this blobNode has already been called");
			}

			DrainType drainType;
			if (GlobalConstants.useDrainData)
				if (drainer.state == DrainerState.FINAL)
					drainType = DrainType.FINAL;
				else
					drainType = DrainType.INTERMEDIATE;
			else
				drainType = DrainType.DISCARD;

			drainer.drain(blobID, drainType);

			// TODO: Verify the waiting time is reasonable.
			if (GlobalConstants.needDrainDeadlockHandler)
				drainer.schExecutorService.schedule(deadLockHandler(), 6000,
						TimeUnit.MILLISECONDS);
		}

		void setDrainData(SNDrainedData drainedData) {
			if (this.snDrainData == null) {
				this.snDrainData = drainedData;
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
}
