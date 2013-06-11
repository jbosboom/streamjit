package edu.mit.streamjit.impl.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import edu.mit.streamjit.api.*;
import edu.mit.streamjit.impl.blob.*;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Portals;
import edu.mit.streamjit.impl.common.VerifyStreamGraph;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.Configuration.*;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.runtime.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.runtime.master.CommunicationManager;
import edu.mit.streamjit.impl.distributed.runtime.master.Master;
import edu.mit.streamjit.impl.distributed.runtime.master.TCPCommunicationManager;
import edu.mit.streamjit.partitioner.*;
import edu.mit.streamjit.util.json.Jsonifiers;
import edu.mit.streamjit.impl.interp.AbstractCompiledStream;
import edu.mit.streamjit.impl.interp.ArrayChannel;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.impl.interp.Interpreter;
import edu.mit.streamjit.impl.interp.SynchronizedChannel;

/**
 * TODO: Now it executes all blobs in a single thread. Need to implement Distributed concurrent blobs soon. TODO:
 * {@link DistributedStreamCompiler} must work with 0 slaves as well. In that case, it should behave like a
 * {@link ConcurrentStreamCompiler}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 6, 2013
 */
public class DistributedStreamCompiler implements StreamCompiler {

	/**
	 * Total number of nodes including master node.
	 */
	int noOfnodes;

	/**
	 * @param noOfnodes
	 *            : Total number of nodes the stream application intended to run - including master node. If it is 1 then it means the
	 *            whole stream application is supposed to run on master.
	 */
	public DistributedStreamCompiler(int noOfnodes) {
		if (noOfnodes < 1)
			throw new IllegalArgumentException("noOfnodes must be 1 or greater");
		this.noOfnodes = noOfnodes;
	}

	/**
	 * Run the whole application on the master node.
	 */
	public DistributedStreamCompiler() {
		this(1);
	}

	@Override
	public <I, O> CompiledStream<I, O> compile(OneToOneElement<I, O> stream) {

		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<I, ?> source = (Worker<I, ?>) primitiveConnector.getSource();
		Worker<?, O> sink = (Worker<?, O>) primitiveConnector.getSink();

		Channel<I> head = new SynchronizedChannel<>(new ArrayChannel<I>());
		Channel<O> tail = new SynchronizedChannel<>(new ArrayChannel<O>());

		Workers.getInputChannels(source).add(head);
		Workers.getOutputChannels(sink).add(tail);

		VerifyStreamGraph verifier = new VerifyStreamGraph();
		stream.visit(verifier);

		Master master = new Master();
		master.connect(noOfnodes - 1);

		Map<Integer, Integer> coreCounts = master.getCoreCount();

		// As we are just running small benchmark applications, lets utilize just a single core per node. TODO: When running big
		// real world applications utilize all available cores. For that simply comment the following lines.
		for (Integer key : coreCounts.keySet()) {
			coreCounts.put(key, 1);
		}

		int totalCores = 0;
		for (int coreCount : coreCounts.values())
			totalCores += coreCount;

		// TODO: Need to map the machines and the partitions precisely. Now just just it is mapped based on the list order.
		Partitioner<I, O> horzPartitioner = new HorizontalPartitioner<>();
		List<Set<Worker<?, ?>>> partitionList = horzPartitioner.PatririonEqually(stream, source, sink, totalCores);
		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = mapPartitionstoMachines(partitionList, coreCounts);

		// TODO: Copied form DebugStreamCompiler. Need to be verified for this context.
		List<MessageConstraint> constraints = MessageConstraint.findConstraints(source);
		Set<Portal<?>> portals = new HashSet<>();
		for (MessageConstraint mc : constraints)
			portals.add(mc.getPortal());
		for (Portal<?> portal : portals)
			Portals.setConstraints(portal, constraints);

		master.setPartition(partitionsMachineMap, stream.getClass().getName(), constraints, source, sink);

		return new DistributedCompiledStream<>(head, tail, master);
	}

	// TODO: Need to do precise mapping. For the moment just mapping in order.
	private Map<Integer, List<Set<Worker<?, ?>>>> mapPartitionstoMachines(List<Set<Worker<?, ?>>> partitionList,
			Map<Integer, Integer> coreCountMap) {

		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = new HashMap<Integer, List<Set<Worker<?, ?>>>>();
		for (Integer machineID : coreCountMap.keySet()) {
			partitionsMachineMap.put(machineID, new ArrayList<Set<Worker<?, ?>>>(coreCountMap.get(machineID)));
		}

		int index = 0;
		for (Integer machineID : partitionsMachineMap.keySet()) {
			if (!(index < partitionList.size()))
				break;
			for (int i = 0; i < coreCountMap.get(machineID); i++) {
				if (!(index < partitionList.size()))
					break;
				partitionsMachineMap.get(machineID).add(partitionList.get(index++));
			}
		}

		// In case we received more partitions than available cores. Assign the remaining partitions in round robin fashion. This case
		// shouldn't happen if the partitioning is correctly done based on the available core count. This code is added just to ensure
		// the correctness of the program and to avoid the bugs.
		while (index < partitionList.size()) {
			for (Integer machineID : partitionsMachineMap.keySet()) {
				if (!(index < partitionList.size()))
					break;
				partitionsMachineMap.get(machineID).add(partitionList.get(index++));
			}
		}
		return partitionsMachineMap;
	}

	private static class DistributedCompiledStream<I, O> extends AbstractCompiledStream<I, O> {

		Master master;

		public DistributedCompiledStream(Channel<? super I> head, Channel<? extends O> tail, Master master) {
			super(head, tail);
			this.master = master;
			this.master.start();
		}

		@Override
		public boolean awaitDraining() throws InterruptedException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean awaitDraining(long timeout, TimeUnit unit) throws InterruptedException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		protected void doDrain() {
			// TODO Auto-generated method stub
		}
	}
}