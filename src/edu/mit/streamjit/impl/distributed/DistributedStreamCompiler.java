package edu.mit.streamjit.impl.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Portal;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.api.Input.ManualInput;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.BlobGraph;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.OutputBufferFactory;
import edu.mit.streamjit.impl.common.Portals;
import edu.mit.streamjit.impl.common.VerifyStreamGraph;
import edu.mit.streamjit.impl.common.BlobGraph.AbstractDrainer;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.CommunicationType;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;
import edu.mit.streamjit.impl.distributed.runtimer.DistributedDrainer;
import edu.mit.streamjit.partitioner.HorizontalPartitioner;
import edu.mit.streamjit.partitioner.Partitioner;

/**
 * 
 * The OneToOneElement that is asked to compile by this {@link Compiler} must be
 * unique. Compilation will fail if default subtypes of the
 * {@link OneToOneElement}s such as {@link Pipeline}, {@link Splitjoin} and etc
 * are be passed.
 * <p>
 * TODO: {@link DistributedStreamCompiler} must work with 1 {@link StreamNode}
 * as well. In that case, it should behave like a
 * {@link ConcurrentStreamCompiler}.
 * </p>
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 6, 2013
 */
public class DistributedStreamCompiler implements StreamCompiler {

	/**
	 * Configuration from Opentuner.
	 */
	Configuration cfg;

	/**
	 * Total number of nodes including controller node.
	 */
	int noOfnodes;

	/**
	 * @param noOfnodes
	 *            : Total number of nodes the stream application intended to run
	 *            - including controller node. If it is 1 then it means the
	 *            whole stream application is supposed to run on controller.
	 */
	public DistributedStreamCompiler(int noOfnodes) {
		if (noOfnodes < 1)
			throw new IllegalArgumentException("noOfnodes must be 1 or greater");
		this.noOfnodes = noOfnodes;
	}

	/**
	 * Run the whole application on the controller node.
	 */
	public DistributedStreamCompiler() {
		this(1);
	}

	/**
	 * Run the application with the passed configureation.
	 */
	public DistributedStreamCompiler(int noOfnodes, Configuration cfg) {
		if (noOfnodes < 1)
			throw new IllegalArgumentException("noOfnodes must be 1 or greater");
		this.noOfnodes = noOfnodes;
		this.cfg = cfg;
	}

	// Having compileConv() and compileConf() is just a temporary hack.
	@Override
	public <I, O> CompiledStream compile(OneToOneElement<I, O> stream,
			Input<I> input, Output<O> output) {
		if (cfg == null)
			return compileConv(stream, input, output);
		return compileConf(stream, input, output);
	}

	public <I, O> CompiledStream compileConv(OneToOneElement<I, O> stream,
			Input<I> input, Output<O> output) {

		checkforDefaultOneToOneElement(stream);

		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<I, ?> source = (Worker<I, ?>) primitiveConnector.getSource();
		Worker<?, O> sink = (Worker<?, O>) primitiveConnector.getSink();

		// TODO: derive a algorithm to find good buffer size and use here.
		Buffer head = InputBufferFactory.unwrap(input).createReadableBuffer(
				1000);
		Buffer tail = OutputBufferFactory.unwrap(output).createWritableBuffer(
				1000);

		ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
				.<Token, Buffer> builder();

		bufferMapBuilder.put(Token.createOverallInputToken(source), head);
		bufferMapBuilder.put(Token.createOverallOutputToken(sink), tail);

		VerifyStreamGraph verifier = new VerifyStreamGraph();
		stream.visit(verifier);

		Map<CommunicationType, Integer> conTypeCount = new HashMap<>();
		conTypeCount.put(CommunicationType.LOCAL, 1);
		conTypeCount.put(CommunicationType.TCP, this.noOfnodes - 1);
		Controller controller = new Controller();
		controller.connect(conTypeCount);

		Map<Integer, Integer> coreCounts = controller.getCoreCount();

		// As we are just running small benchmark applications, lets utilize
		// just a single core per node. TODO: When running big
		// real world applications utilize all available cores. For that simply
		// comment the following lines.
		for (Integer key : coreCounts.keySet()) {
			coreCounts.put(key, 1);
		}

		int totalCores = 0;
		for (int coreCount : coreCounts.values())
			totalCores += coreCount;

		// TODO: Need to map the machines and the partitions precisely. Now just
		// it is mapped based on the list order.
		Partitioner<I, O> horzPartitioner = new HorizontalPartitioner<>();
		List<Set<Worker<?, ?>>> partitionList = horzPartitioner
				.partitionEqually(stream, source, sink, totalCores);
		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = mapPartitionstoMachines(
				partitionList, coreCounts);

		BlobGraph bg = new BlobGraph(partitionList);

		// TODO: Copied form DebugStreamCompiler. Need to be verified for this
		// context.
		List<MessageConstraint> constraints = MessageConstraint
				.findConstraints(source);
		Set<Portal<?>> portals = new HashSet<>();
		for (MessageConstraint mc : constraints)
			portals.add(mc.getPortal());
		for (Portal<?> portal : portals)
			Portals.setConstraints(portal, constraints);

		String jarFilePath = this.getClass().getProtectionDomain()
				.getCodeSource().getLocation().getPath();

		controller.setPartition(partitionsMachineMap, jarFilePath, stream
				.getClass().getName(), constraints, source, sink,
				bufferMapBuilder.build(), cfg);

		final DistributedCompiledStream cs = new DistributedCompiledStream(bg,
				controller);

		if (input instanceof ManualInput)
			InputBufferFactory
					.setManualInputDelegate(
							(ManualInput<I>) input,
							new InputBufferFactory.AbstractManualInputDelegate<I>(
									head) {
								@Override
								public void drain() {
									cs.drain();
								}
							});
		else
			cs.drain();
		return cs;
	}

	public <I, O> CompiledStream compileConf(OneToOneElement<I, O> stream,
			Input<I> input, Output<O> output) {

		checkforDefaultOneToOneElement(stream);

		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<I, ?> source = (Worker<I, ?>) primitiveConnector.getSource();
		Worker<?, O> sink = (Worker<?, O>) primitiveConnector.getSink();

		// TODO: derive a algorithm to find good buffer size and use here.
		Buffer head = InputBufferFactory.unwrap(input).createReadableBuffer(
				1000);
		Buffer tail = OutputBufferFactory.unwrap(output).createWritableBuffer(
				1000);

		ImmutableMap.Builder<Token, Buffer> bufferMapBuilder = ImmutableMap
				.<Token, Buffer> builder();

		bufferMapBuilder.put(Token.createOverallInputToken(source), head);
		bufferMapBuilder.put(Token.createOverallOutputToken(sink), tail);

		VerifyStreamGraph verifier = new VerifyStreamGraph();
		stream.visit(verifier);

		ImmutableSet<Worker<?, ?>> allworkers = Workers
				.getAllWorkersInGraph(source);

		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = getMachineWorkerMap(
				cfg, allworkers);

		List<Set<Worker<?, ?>>> partitionList = new ArrayList<>();
		for (List<Set<Worker<?, ?>>> lst : partitionsMachineMap.values()) {
			partitionList.addAll(lst);
		}

		BlobGraph bg = null;
		try {
			bg = new BlobGraph(partitionList);
		} catch (StreamCompilationFailedException ex) {
			System.err.print("Cycles found in the worker->blob assignment");

			throw ex;
		}

		for (int machine : partitionsMachineMap.keySet()) {
			System.err.print("\nMachine - " + machine);
			for (Set<Worker<?, ?>> blobworkers : partitionsMachineMap
					.get(machine)) {
				System.err.print("\n\tBlob worker set : ");
				for (Worker<?, ?> w : blobworkers) {
					System.err.print(Workers.getIdentifier(w) + " ");
				}
			}
		}
		System.err.println();

		int nodeCount = partitionsMachineMap.keySet().size();

		Map<CommunicationType, Integer> conTypeCount = new HashMap<>();
		conTypeCount.put(CommunicationType.LOCAL, 1);
		conTypeCount.put(CommunicationType.TCP, nodeCount - 1);
		Controller controller = new Controller();
		controller.connect(conTypeCount);

		// TODO: Copied form DebugStreamCompiler. Need to be verified for this
		// context.
		List<MessageConstraint> constraints = MessageConstraint
				.findConstraints(source);
		Set<Portal<?>> portals = new HashSet<>();
		for (MessageConstraint mc : constraints)
			portals.add(mc.getPortal());
		for (Portal<?> portal : portals)
			Portals.setConstraints(portal, constraints);

		String jarFilePath = this.getClass().getProtectionDomain()
				.getCodeSource().getLocation().getPath();

		controller.setPartition(partitionsMachineMap, jarFilePath, stream
				.getClass().getName(), constraints, source, sink,
				bufferMapBuilder.build(), cfg);

		final DistributedCompiledStream cs = new DistributedCompiledStream(bg,
				controller);

		if (input instanceof ManualInput)
			InputBufferFactory
					.setManualInputDelegate(
							(ManualInput<I>) input,
							new InputBufferFactory.AbstractManualInputDelegate<I>(
									head) {
								@Override
								public void drain() {
									cs.drain();
								}
							});
		else
			cs.drain();
		return cs;
	}

	/**
	 * Reads the configuration and returns a map of nodeID to list of workers
	 * set which are assigned to the node. value of the returned map is list of
	 * worker set where each worker set is individual blob.
	 * 
	 * @param config
	 * @param workerset
	 * @return map of nodeID to list of workers set which are assigned to the
	 *         node. value is list of worker set where each set is individual
	 *         blob.
	 */
	private Map<Integer, List<Set<Worker<?, ?>>>> getMachineWorkerMap(
			Configuration config, ImmutableSet<Worker<?, ?>> workerset) {
		Map<Integer, Set<Worker<?, ?>>> partition = new HashMap<>();
		for (Worker<?, ?> w : workerset) {
			IntParameter w2m = config.getParameter(String.format(
					"worker%dtomachine", Workers.getIdentifier(w)),
					IntParameter.class);
			int machine = w2m.getValue();

			if (!partition.containsKey(machine)) {
				Set<Worker<?, ?>> set = new HashSet<>();
				partition.put(machine, set);
			}
			partition.get(machine).add(w);
		}

		Map<Integer, List<Set<Worker<?, ?>>>> machineWorkerMap = new HashMap<>();
		for (int machine : partition.keySet()) {
			machineWorkerMap.put(machine, getBlobs(partition.get(machine)));
		}
		return machineWorkerMap;
	}

	/**
	 * Goes through all the workers assigned to a machine, find the workers
	 * which are interconnected and group them as a blob workers. i.e., Group
	 * the workers such that each group can be executed as either a compiler
	 * blob or an interpreter blob.
	 * <p>
	 * TODO: Not implemented yet. If any dynamic edges exists then should create
	 * interpreter blob.
	 * 
	 * @param workerset
	 * @return list of workers set which contains interconnected workers.
	 */
	private List<Set<Worker<?, ?>>> getBlobs(Set<Worker<?, ?>> workerset) {
		List<Set<Worker<?, ?>>> ret = new ArrayList<Set<Worker<?, ?>>>();
		ret.add(workerset);
		return ret;
	}

	// TODO: Need to do precise mapping. For the moment just mapping in order.
	private Map<Integer, List<Set<Worker<?, ?>>>> mapPartitionstoMachines(
			List<Set<Worker<?, ?>>> partitionList,
			Map<Integer, Integer> coreCountMap) {

		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = new HashMap<Integer, List<Set<Worker<?, ?>>>>();
		for (Integer machineID : coreCountMap.keySet()) {
			partitionsMachineMap.put(
					machineID,
					new ArrayList<Set<Worker<?, ?>>>(coreCountMap
							.get(machineID)));
		}

		int index = 0;
		for (Integer machineID : partitionsMachineMap.keySet()) {
			if (!(index < partitionList.size()))
				break;
			for (int i = 0; i < coreCountMap.get(machineID); i++) {
				if (!(index < partitionList.size()))
					break;
				partitionsMachineMap.get(machineID).add(
						partitionList.get(index++));
			}
		}

		// In case we received more partitions than available cores. Assign the
		// remaining partitions in round robin fashion. This case
		// shouldn't happen if the partitioning is correctly done based on the
		// available core count. This code is added just to ensure
		// the correctness of the program and to avoid the bugs.
		while (index < partitionList.size()) {
			for (Integer machineID : partitionsMachineMap.keySet()) {
				if (!(index < partitionList.size()))
					break;
				partitionsMachineMap.get(machineID).add(
						partitionList.get(index++));
			}
		}
		return partitionsMachineMap;
	}

	/**
	 * TODO: Need to check for other default subtypes of {@link OneToOneElement}
	 * s. Now only checks for first generation children.
	 * 
	 * @param stream
	 * @throws StreamCompilationFailedException
	 *             if stream is default subtype of OneToOneElement
	 */
	private <I, O> void checkforDefaultOneToOneElement(
			OneToOneElement<I, O> stream) {

		if (stream.getClass() == Pipeline.class
				|| stream.getClass() == Splitjoin.class
				|| stream.getClass() == Filter.class) {
			throw new StreamCompilationFailedException(
					"Default subtypes of OneToOneElement are not accepted for compilation by this compiler. OneToOneElement that passed should be unique");
		}
	}

	private static class DistributedCompiledStream implements CompiledStream {

		Controller controller;
		AbstractDrainer drainer;

		public DistributedCompiledStream(BlobGraph blobGraph,
				Controller controller) {
			this.controller = controller;
			this.drainer = new DistributedDrainer(blobGraph, false, controller);
			this.controller.start();
		}

		@Override
		public boolean isDrained() {
			return drainer.isDrained();
		}

		private void drain() {
			drainer.startDraining();
		}

		@Override
		public void awaitDrained() throws InterruptedException {
			drainer.awaitDrained();

		}

		@Override
		public void awaitDrained(long timeout, TimeUnit unit)
				throws InterruptedException, TimeoutException {
			drainer.awaitDrained(timeout, unit);
		}
	}
}