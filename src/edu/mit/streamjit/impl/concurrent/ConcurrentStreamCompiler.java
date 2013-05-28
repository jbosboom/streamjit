package edu.mit.streamjit.impl.concurrent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Portal;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Portals;
import edu.mit.streamjit.impl.common.VerifyStreamGraph;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.interp.AbstractCompiledStream;
import edu.mit.streamjit.impl.interp.ArrayChannel;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.impl.interp.ChannelFactory;
import edu.mit.streamjit.impl.interp.SynchronizedChannel;
import edu.mit.streamjit.partitioner.HorizontalPartitioner;
import edu.mit.streamjit.partitioner.Partitioner;

/**
 * A stream compiler that partitions a streamgraph into multiple blobs and execute it on multiple threads.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Apr 8, 2013
 */
public class ConcurrentStreamCompiler implements StreamCompiler {
	int noOfBlobs;

	/**
	 * @param Patrions
	 *            a stream graph up to noOfBlobs many blobs and executes each blob on each thread.
	 */
	public ConcurrentStreamCompiler(int noOfBlobs) {
		if(noOfBlobs < 1)
			throw new IllegalArgumentException("noOfBlobs should be 1 or greater");
		this.noOfBlobs = noOfBlobs;
	}

	@Override
	public <I, O> CompiledStream<I, O> compile(OneToOneElement<I, O> stream) {
		
		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<I, ?> source = (Worker<I, ?>) primitiveConnector.getSource();
		Worker<?, O> sink = (Worker<?, O>) primitiveConnector.getSink();
		
		VerifyStreamGraph verifier = new VerifyStreamGraph();
		stream.visit(verifier);
		
		Partitioner<I, O> horzPartitioner = new HorizontalPartitioner<>();
		List<Set<Worker<?, ?>>> partitionList = horzPartitioner.PatririonEqually(stream, source, sink, this.noOfBlobs);

		List<Blob> blobList = makeBlobs(partitionList, source, sink);
		Channel<I> head = (Channel<I>) Workers.getInputChannels(source).get(0);
		Channel<O> tail = (Channel<O>) Workers.getOutputChannels(sink).get(0);

		// TODO: Copied form DebugStreamCompiler. Need to be verified for this context.
		List<MessageConstraint> constraints = MessageConstraint.findConstraints(source);
		Set<Portal<?>> portals = new HashSet<>();
		for (MessageConstraint mc : constraints)
			portals.add(mc.getPortal());
		for (Portal<?> portal : portals)
			Portals.setConstraints(portal, constraints);

		return new ConcurrentCompiledStream<>(head, tail, blobList);
	}

	private <I, O> List<Blob> makeBlobs(List<Set<Worker<?, ?>>> partitionList, Worker<I, ?> source, Worker<?, O> sink) {
		PartitionVisitor partitionVisitor = new PartitionVisitor(new IntraBlobChannelFactory(), new InterBlobChannelFactory());

		for (Set<Worker<?, ?>> partition : partitionList) {
			partitionVisitor.visitPartition(partition);
		}

		partitionVisitor.visitSource(source, false);
		partitionVisitor.visitSink(sink, false);

		BlobFactory blobFactory = new SingleThreadedFactory();
		List<Blob> blobList = new LinkedList<>();
		for (Set<Worker<?, ?>> partition : partitionList) {
			blobList.add(blobFactory.makeBlob(partition, null, 1));
		}

		return blobList;
	}

	private class IntraBlobChannelFactory implements ChannelFactory {

		@Override
		public <E> Channel<E> makeChannel(Worker<?, E> upstream, Worker<E, ?> downstream) {
			return new ArrayChannel<E>();
			// return new SynchronizedChannel<>(new ArrayChannel<E>());
		}
	}

	private class InterBlobChannelFactory implements ChannelFactory {

		@Override
		public <E> Channel<E> makeChannel(Worker<?, E> upstream, Worker<E, ?> downstream) {
			return new SynchronizedChannel<>(new ArrayChannel<E>());
		}
	}

	private static class ConcurrentCompiledStream<I, O> extends AbstractCompiledStream<I, O> {
		List<Blob> blobList;
		List<Thread> blobThreads;

		public ConcurrentCompiledStream(Channel<? super I> head, Channel<? extends O> tail, List<Blob> blobList) {
			super(head, tail);
			this.blobList = blobList;
			blobThreads = new ArrayList<>(this.blobList.size());
			for (Blob b : blobList) {
				blobThreads.add(new Thread(b.getCoreCode(0)));
			}
			start();
		}

		/*
		 * If CompiledStream provides start() interface function then make this public. Currently start() is called inside the
		 * ConcurrentCompiledStream's constructor.
		 */
		private void start() {
			for (Thread t : blobThreads) {
				t.start();
			}
		}

		@Override
		protected void doDrain() {
			new DrainerCallback(blobList).run();
		}

		@Override
		public boolean awaitDraining() throws InterruptedException {
			for (Thread t : blobThreads) {
				try {
					t.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
					throw e;
				}
			}
			return true;
		}

		@Override
		public boolean awaitDraining(long timeout, TimeUnit unit) throws InterruptedException {
			long milliTimeout = unit.toMillis(timeout);
			for (Thread t : blobThreads) {
				try {
					t.join(milliTimeout);
				} catch (InterruptedException e) {
					e.printStackTrace();
					throw e;
				}
			}
			return true;
		}
	}
}