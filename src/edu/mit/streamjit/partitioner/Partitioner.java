package edu.mit.streamjit.partitioner;

import java.util.List;
import java.util.Set;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.concurrent.BlobVisitor;

/**
 * {@link Partitioner} is to partitions a stream graph (i.e.
 * {@link OneToOneElement}) to multiple non overlapped chunks.
 * 
 * Note: In prior to get service from {@link BlobVisitor}, all workers in the
 * stream graph should be connected by setting all
 * predecessors, successors. Consider using {@link ConnectWorkersVisitor} to set
 * all predecessors, successors, relationships before
 * start partitioning
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 2, 2013
 */
public interface Partitioner<I, O> {

	/**
	 * partitions a stream graph (i.e. {@link OneToOneElement}) in to equal non
	 * overlapping chunks. If it is not possible to split the
	 * stream graph in to exactly equal chunks, then the last partition can be
	 * the residue.
	 */
	public List<Set<Worker<?, ?>>> partitionEqually(
			OneToOneElement<I, O> streamGraph, Worker<I, ?> source,
			Worker<?, O> sink, int noOfPartitions);
}
