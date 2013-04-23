package edu.mit.streamjit.impl.concurrent;

import java.util.HashSet;
import java.util.Set;

import com.sun.corba.se.spi.orbutil.threadpool.Work;

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
 * {@link PartitionVisitor} visits through a partitioned set of workers and connects by {@link Channel}s. If a worker and it's
 * successor happened to fall into same blob then connects them with intraBlobChannelFactory's {@link Channel}. If worker and it's
 * successor are in different partition then connects it with interChannelFactory's {@link Channel}.
 * 
 * Note: In prior to get service from {@link PartitionVisitor}, all workers in the stream graph should be connected by setting all
 * predecessors, successors. Consider using {@link ConnectWorkersVisitor} to set all predecessors, successors, relationships before
 * start partitioning
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 8, 2013
 */
public class PartitionVisitor extends StreamVisitor {

	Set<Worker<?, ?>> partitionWorkers;
	Set<Worker<?, ?>> visitedWorkers;

	Worker<?, ?> source;
	Worker<?, ?> sink;

	static private int identifier = 0;

	ChannelFactory intraBlobChannelFactory;
	ChannelFactory interBlobChannelFactory;

	PartitionVisitor(ChannelFactory intraBlobChannelFactory, ChannelFactory interBlobChannelFactory) {
		this.intraBlobChannelFactory = intraBlobChannelFactory;
		this.interBlobChannelFactory = interBlobChannelFactory;
	}

	public void visitPartition(Set<Worker<?, ?>> blobWorkers) {
		this.partitionWorkers = blobWorkers;
		visitedWorkers = new HashSet<Worker<?, ?>>();
		for (Worker<?, ?> cur : blobWorkers) {
			cur.visit(this);
		}
	}

	public <I> void visitSource(Worker<I, ?> source, boolean onMaterBlob) {
		assert Workers.getAllPredecessors(source).isEmpty() : "Expected source to be passed. But got a non source worker";
		Channel<I> c;
		if (onMaterBlob)
			c = intraBlobChannelFactory.makeChannel(null, source);
		else
			c = interBlobChannelFactory.makeChannel(null, source);

		Workers.getInputChannels(source).add(c);
	}

	public <O> void visitSink(Worker<?, O> sink, boolean onMaterBlob) {
		assert Workers.getAllSuccessors(sink).isEmpty() : "Expected sink to be passed. But got a non source worker";
		Channel<O> c;
		if (onMaterBlob)
			c = intraBlobChannelFactory.makeChannel(sink, null);
		else
			c = interBlobChannelFactory.makeChannel(sink, null);

		Workers.getOutputChannels(sink).add(c);
	}

	@Override
	public void beginVisit() {
		// TODO Auto-generated method stub

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
		// TODO Auto-generated method stub
	}

	@Override
	public boolean enterSplitjoin(Splitjoin<?, ?> splitjoin) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void visitSplitter(Splitter<?> splitter) {
		visitWorker(splitter);
	}

	@Override
	public boolean enterSplitjoinBranch(OneToOneElement<?, ?> element) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void exitSplitjoinBranch(OneToOneElement<?, ?> element) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visitJoiner(Joiner<?> joiner) {
		visitWorker(joiner);
	}

	@Override
	public void exitSplitjoin(Splitjoin<?, ?> splitjoin) {
		// TODO Auto-generated method stub

	}

	@Override
	public void endVisit() {
		// TODO Auto-generated method stub
	}

	/**
	 * Connects worker with it's all successors if the worker and the successor happen to fall into same blob. Skip otherwise. TODO:
	 * successor should be Worker<E, ?>. It shows bug in Eclipse. Check it with Netbeans. Now raw type is used.
	 */
	private <E> void visitWorker(Worker<?, E> worker) {
		if (!partitionWorkers.contains(worker))
			return;
		Workers.setIdentifier(worker, identifier++);

		for (Worker successor : Workers.getSuccessors(worker)) {
			if (partitionWorkers.contains(successor))
				intraConnect(worker, successor);
			else
				interConnect(worker, successor);
		}
	}

	private <E> void intraConnect(Worker<?, E> source, Worker<E, ?> destination) {
		assert partitionWorkers.contains(source) : "Illegal assignment: source worker is not in the current blob";
		assert partitionWorkers.contains(destination) : "Illegal assignment: destination worker is not in the current blob";

		int srcIdx = Workers.getSuccessors(source).indexOf(destination);
		int dstIdx = Workers.getPredecessors(destination).indexOf(source);

		assert Workers.getOutputChannels(source).get(srcIdx) == null : "Fetal Error: Output channel is already set.";
		assert Workers.getInputChannels(destination).get(dstIdx) == null : "Fetal Error: Output channel is already set.";

		Channel<E> chnl = this.intraBlobChannelFactory.makeChannel(source, destination);
		Workers.getOutputChannels(source).set(srcIdx, chnl);
		Workers.getInputChannels(destination).set(dstIdx, chnl);
	}

	private <E> void interConnect(Worker<?, E> source, Worker<E, ?> destination) {
		assert partitionWorkers.contains(source) : "Illegal assignment: source worker is not in the current blob";
		assert !partitionWorkers.contains(destination) : "Illegal assignment: destination worker is in the current blob";

		int srcIdx = Workers.getSuccessors(source).indexOf(destination);
		int dstIdx = Workers.getPredecessors(destination).indexOf(source);

		assert Workers.getOutputChannels(source).get(srcIdx) == null : "Fetal Error: Output channel is already set.";
		assert Workers.getInputChannels(destination).get(dstIdx) == null : "Fetal Error: Output channel is already set.";

		Channel<E> chnl = this.interBlobChannelFactory.makeChannel(source, destination);
		Workers.getOutputChannels(source).set(srcIdx, chnl);
		Workers.getInputChannels(destination).set(dstIdx, chnl);
	}
}
