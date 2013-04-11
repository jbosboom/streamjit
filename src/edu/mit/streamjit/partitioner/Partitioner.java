package edu.mit.streamjit.partitioner;

import java.util.List;
import java.util.Set;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.concurrent.PartitionVisitor;

/**
 * {@link Partitioner} is to partitions a stream graph (i.e. {@link OneToOneElement}) to multiple non overlapped chunks. TODO: provides
 * two interface functions getSource(), getSink(). Consider provide these at {@link OneToOneElement}. That would be a perfect place.
 * 
 * Note: In prior to get service from {@link PartitionVisitor}, all workers in the stream graph should be connected by setting all
 * predecessors, successors. Consider using {@link ConnectWorkersVisitor} to set all predecessors, successors, relationships before
 * start partitioning
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 2, 2013
 */
public interface Partitioner<I, O> {

	/**
	 * partitions a stream graph (i.e. {@link OneToOneElement}) in to equal non overlapping chunks. If it is not possible to split the
	 * stream graph in to exactly equal chunks, then the last partition can be the reminder.
	 */
	public List<Set<Worker<?, ?>>> PatririonEqually(OneToOneElement<I, O> streamGraph, int noOfPartitions);

	/**
	 * returns the source of the stream graph. When partitioning a stream graph, finding the source of the stream graph is easier.
	 */
	public Worker<I, ?> getSource();

	/**
	 * returns the sink of the stream
	 * 
	 */
	public Worker<?, O> getSink();
}
