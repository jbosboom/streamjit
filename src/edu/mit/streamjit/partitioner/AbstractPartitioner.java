package edu.mit.streamjit.partitioner;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.Workers;

/**
 * {@link AbstractPartitioner} does not implement any of {@link Partitioner}'s methods. Instead, it does common pre-processing and
 * post-processing on stream graph that is needed by all {@link Partitioner} classes.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 8, 2013
 */
public abstract class AbstractPartitioner<I, O> implements Partitioner<I, O> {

	protected Worker<I, ?> source;
	protected Worker<?, O> sink;

	public Worker<I, ?> getSource() {
		return source;
	}

	public Worker<?, O> getSink() {
		return sink;
	}

	protected int graphWidth;
	/**
	 * The Deepest depth of the graph.
	 */
	protected int graphDepth;

	/**
	 * Calculates and returns the depth of a stream graph.
	 * 
	 * @param source
	 *            : Source of the stream graph.
	 * @return Depth of the stream graph.
	 */
	protected int getDepthofStreamGraph(Worker<?, ?> source) {
		int depth = 0; // source is at depth 1
		Worker<?, ?> cur = source;

		while (!Workers.getSuccessors(cur).isEmpty()) {
			if (cur instanceof Splitter<?>) {
				depth += getDepthofSplitJoin((Splitter<?>) cur);
				cur = getJoiner((Splitter<?>) cur);
			} else if (cur instanceof Worker<?, ?>) {
				depth++;
				cur = Workers.getSuccessors(cur).get(0);
			} else if (cur instanceof Joiner<?>) {
				cur = Workers.getSuccessors(cur).get(0);
			} else {
				throw new AssertionError("Unexpected worker found. Verify the algorithm");
			}
		}
		return depth;
	}

	/**
	 * Calculate and returns the depth of a {@link Splitjoin}. This function handles unbalanced branches in the {@link Splitjoin} and
	 * inner/nested {@link Splitjoin}s as well. This is a recursive function.
	 * 
	 * @param splitter
	 *            : {@link Splitter} of the {@link Splitjoin} which's depth is required by the caller.
	 * @return Depth of the {@link Splitjoin}.
	 */
	protected int getDepthofSplitJoin(Splitter<?> splitter) {
		Joiner<?> joiner = getJoiner(splitter);
		int branchCount = Workers.getSuccessors(splitter).size();
		int[] branchDepths = new int[branchCount]; // Java initializes the array values to 0 by default.
		for (int i = 0; i < branchCount; i++) {
			branchDepths[i]++; // This increment counts the splitter
			Worker<?, ?> cur = Workers.getSuccessors(splitter).get(i);
			while (!cur.equals(joiner)) {
				if (cur instanceof Splitter<?>) {
					branchDepths[i] += getDepthofSplitJoin((Splitter<?>) cur);
					cur = Workers.getSuccessors(getJoiner((Splitter<?>) cur)).get(0);
				} else if (cur instanceof Filter<?, ?>) {
					branchDepths[i]++;
					cur = Workers.getSuccessors(cur).get(0);
				} else {
					throw new AssertionError("a Joiner found. Check the algorithm");
				}
			}
			branchDepths[i]++; // This increment counts the joiner
		}
		return getMax(branchDepths);
	}

	/**
	 * Find and returns the corresponding {@link Joiner} for the passed {@link Splitter}.
	 * 
	 * @param splitter
	 *            : {@link Splitter} that needs it's {@link Joiner}.
	 * @return Corresponding {@link Joiner} of the passed {@link Splitter}.
	 */
	protected Joiner<?> getJoiner(Splitter<?> splitter) {
		Worker<?, ?> cur = Workers.getSuccessors(splitter).get(0);
		int innerSplitjoinCount = 0;
		while (!(cur instanceof Joiner<?>) || innerSplitjoinCount != 0) {
			if (cur instanceof Splitter<?>)
				innerSplitjoinCount++;
			if (cur instanceof Joiner<?>)
				innerSplitjoinCount--;
			assert innerSplitjoinCount >= 0 : "Joiner Count is more than splitter count. Check the algorithm";
			cur = Workers.getSuccessors(cur).get(0);
		}
		assert cur instanceof Joiner<?> : "Error in algorithm. Not returning a Joiner";
		return (Joiner<?>) cur;
	}

	/**
	 * Returns all {@link Filter}s in a splitjoin. Does not include the splitter or the joiner. This function doesn't support nested
	 * splitjoins.
	 * 
	 * @param splitter
	 * @return Returns all {@link Filter}s in a splitjoin. Does not include splitter or joiner.
	 * @throws IllegalArgumentException
	 *             If nested splitjoin is passed.
	 */
	protected Set<Worker<?, ?>> getAllChildWorkers(Splitter<?> splitter) {
		Set<Worker<?, ?>> childWorkers = new HashSet<>();
		Worker<?, ?> cur;
		for (Worker<?, ?> childWorker : Workers.getSuccessors(splitter)) {
			cur = childWorker;
			while (cur instanceof Filter<?, ?>) {
				childWorkers.add(cur);
				assert Workers.getSuccessors(cur).size() == 1 : "Illegal Filter encounted";
				cur = Workers.getSuccessors(cur).get(0);
			}
			if (cur instanceof Splitter<?>) {
				throw new IllegalArgumentException(
						"More complex splitjoiner found. Current Splitjoiner contains another splitjoin inside.");
			}
		}
		return childWorkers;
	}

	/**
	 * Verify the partitionlist for any duplicate workers in more than one partition or any missing workers. Partition list should
	 * completely satisfy the all workers in the stream graph with no duplication or no misses. *
	 * 
	 * @param partitionList
	 *            : list of partitions to be verified.
	 * @return
	 */
	protected boolean verifyPartition(List<Set<Worker<?, ?>>> partitionList) {
		List<Worker<?, ?>> workersInPartitionList = new LinkedList<>();
		for (Set<Worker<?, ?>> partition : partitionList) {
			workersInPartitionList.addAll(partition);
		}
		Set<Worker<?, ?>> allWorkers = Workers.getAllWorkersInGraph(source);

		if (allWorkers.size() > workersInPartitionList.size())
			throw new AssertionError("Wrong partition: Possibly workers are missed");

		if (workersInPartitionList.size() > allWorkers.size())
			throw new AssertionError("Wrong partition: Possibly workers are duplicated");

		if (allWorkers.size() == workersInPartitionList.size() && !allWorkers.containsAll(workersInPartitionList))
			throw new AssertionError("Wrong partition: Possibly workers are duplicated and missed");

		return true;
	}

	/**
	 * Sets the predecessor-successor relationship in the stream graph. It never create a any channels. See the comment of
	 * {@link ConnectWorkersVisitor} some additional details.
	 */
	protected void preProcessStreamGraph(OneToOneElement<I, O> streamGraph) {
		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		streamGraph.visit(primitiveConnector);
		source = (Worker<I, ?>) primitiveConnector.getSource();
		sink = (Worker<?, O>) primitiveConnector.getSink();
		graphDepth = getDepthofStreamGraph(source);
	}

	private int getMax(int[] intArray) {
		int max = Integer.MIN_VALUE;
		for (int i = 0; i < intArray.length; i++) {
			if (intArray[i] > max)
				max = intArray[i];
		}
		return max;
	}
}
