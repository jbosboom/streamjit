package edu.mit.streamjit.impl.concurrent;

import java.util.HashSet;
import java.util.Set;

import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamVisitor;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.impl.interp.ChannelFactory;

/**
 * {@link BlobVisitor} visits through a set of workers of a {@link Blob} and connects them by {@link Channel}s. {@link Channel} manufactured
 * by {@link ConcurrentChannelFactory} is used to make connection.
 * 
 * Note: In prior to get service from {@link BlobVisitor}, all workers in the stream graph should be connected by setting all
 * predecessors, successors. Consider using {@link ConnectWorkersVisitor} to set all predecessors, successors, relationships before
 * start partitioning
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 8, 2013
 */
public class BlobVisitor extends StreamVisitor {

	Set<Worker<?, ?>> partitionWorkers;
	Set<Worker<?, ?>> visitedWorkers;

	Worker<?, ?> source;
	Worker<?, ?> sink;

	ChannelFactory channelFactory;

	public void visitBlob(Set<Worker<?, ?>> blobWorkers) {
		this.partitionWorkers = blobWorkers;
		this.channelFactory = new ConcurrentChannelFactory(blobWorkers);
		visitedWorkers = new HashSet<Worker<?, ?>>();
		for (Worker<?, ?> cur : blobWorkers) {
			cur.visit(this);
		}
	}

	@Override
	public void beginVisit() {
	}

	@Override
	public void visitFilter(Filter<?, ?> filter) {
		visitWorker(filter);
	}

	@Override
	public boolean enterPipeline(Pipeline<?, ?> pipeline) {
		if (partitionWorkers.contains(pipeline))
			return true;
		else
			return false;
	}

	@Override
	public void exitPipeline(Pipeline<?, ?> pipeline) {
	}

	@Override
	public boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
		return true;
	}

	@Override
	public void visitSplitter(Splitter<?> splitter) {
		visitWorker(splitter);
	}

	@Override
	public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
		return true;
	}

	@Override
	public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {

	}

	@Override
	public void visitJoiner(Joiner<?> joiner) {
		visitWorker(joiner);
	}

	@Override
	public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
	}

	@Override
	public void endVisit() {
	}

	/**
	 * TODO: successor should be Worker<E, ?>. It shows bug in Eclipse. Check it with Netbeans. Now raw type is used.
	 */
	private <E> void visitWorker(Worker<?, E> worker) {
		if (!partitionWorkers.contains(worker))
			return;

		for (Worker successor : Workers.getSuccessors(worker)) {

			int srcIdx = Workers.getSuccessors(worker).indexOf(successor);
			int dstIdx = Workers.getPredecessors(successor).indexOf(worker);

			assert Workers.getOutputChannels(worker).get(srcIdx) == null : "Fetal Error: Output channel is already set.";
			assert Workers.getInputChannels(successor).get(dstIdx) == null : "Fetal Error: Output channel is already set.";

			Channel<E> chnl = this.channelFactory.makeChannel(worker, successor);

			Workers.getOutputChannels(worker).set(srcIdx, chnl);
			Workers.getInputChannels(successor).set(dstIdx, chnl);
		}
	}
}
