package edu.mit.streamjit.partitioner;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import edu.mit.streamjit.api.OneToOneElement;
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

	Worker<I, ?> source;
	Worker<?, O> sink;
	int graphDepth;
	int graphWidth;

	/**
	 * Sets the predecessor-successor relationship in the stream graph. It never create a any channels. See the comment of
	 * {@link ConnectWorkersVisitor} some additional details.
	 */
	protected void preProcessStreamGraph(OneToOneElement<I, O> streamGraph) {
		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		streamGraph.visit(primitiveConnector);
		source = (Worker<I, ?>) primitiveConnector.getSource();
		sink = (Worker<?, O>) primitiveConnector.getSink();
		graphDepth = calculateStreamGraphDepth();
	}

	/**
	 * returns the depth of the stream graph. if the branches of a splitjoiner have unequal depth, then the highest depth branch will
	 * be counted.
	 */
	private int calculateStreamGraphDepth() {
		int depth = 1; // source is at depth 1
		Worker<?, ?> cur = source;

		while (!Workers.getSuccessors(cur).isEmpty()) { // TODO : need to handle the uneven spilitjoin branches
			depth++;
			cur = Workers.getSuccessors(cur).get(0);
		}
		return depth;
	}

	/**
	 * Verify the partitionlist for any duplicate workers in more than one partition or any missing workers. Partition list should
	 * completely satisfy the all workers in the stream graph with no duplication or no misses. *
	 * 
	 * @param partitionList
	 *            : list of partitions to be verified.
	 * @return
	 */
	public boolean verifyPartition(List<Set<Worker<?, ?>>> partitionList) {
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

	public Worker<I, ?> getSource() {
		return source;
	}

	public Worker<?, O> getSink() {
		return sink;
	}
}
