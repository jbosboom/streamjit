package edu.mit.streamjit.impl.distributed;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.mit.streamjit.api.*;
import edu.mit.streamjit.impl.blob.*;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Portals;
import edu.mit.streamjit.impl.common.VerifyStreamGraph;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.Configuration.*;
import edu.mit.streamjit.impl.distributed.runtime.master.CommunicationManager;
import edu.mit.streamjit.impl.distributed.runtime.master.TCPCommunicationManager;
import edu.mit.streamjit.partitioner.*;
import edu.mit.streamjit.util.json.Jsonifiers;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.impl.interp.Interpreter;

/**
 * TODO: Executes all blobs on single thread. Need to implement Distributed concurrent blobs soon.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 6, 2013
 */
public class DistributedStreamCompiler implements StreamCompiler {

	int noOfSlaves;

	public DistributedStreamCompiler(int noOfSlaves) {
		if (noOfSlaves < 1)
			throw new IllegalArgumentException("noOfBlobs should be 1 or greater");
		this.noOfSlaves = noOfSlaves;
	}

	@Override
	public <I, O> CompiledStream<I, O> compile(OneToOneElement<I, O> stream) {

		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		stream.visit(primitiveConnector);
		Worker<I, ?> source = (Worker<I, ?>) primitiveConnector.getSource();
		Worker<?, O> sink = (Worker<?, O>) primitiveConnector.getSink();

		VerifyStreamGraph verifier = new VerifyStreamGraph();
		stream.visit(verifier);

		CommunicationManager comManager = new TCPCommunicationManager();
		// TODO: Need to handle this exception well.
		try {
			comManager.connectMachines(this.noOfSlaves);
		} catch (IOException e) {
			System.out.println("Connection Error...");
			e.printStackTrace();
			System.exit(0);
		}

		// Need to get core counts here....
		Map<Integer, Integer> coreCounts = comManager.getCoreCount();
		int totalCores = 0;
		for (int coreCount : coreCounts.values())
			totalCores += coreCount;

		// TODO: Need to map the machines and the partitions precisely. Now just just it is mapped based on the list order.
		Partitioner<I, O> horzPartitioner = new HorizontalPartitioner<>();
		// One blob per machine.
		List<Set<Worker<?, ?>>> partitionList = horzPartitioner.PatririonEqually(stream, source, sink, comManager
				.getConnectedMachineIDs().size());
		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = mapPartitionstoMachines(partitionList,
				comManager.getConnectedMachineIDs());

		Channel<I> head = (Channel<I>) Workers.getInputChannels(source).get(0);
		Channel<O> tail = (Channel<O>) Workers.getOutputChannels(sink).get(0);

		// TODO: Copied form DebugStreamCompiler. Need to be verified for this context.
		List<MessageConstraint> constraints = MessageConstraint.findConstraints(source);
		Set<Portal<?>> portals = new HashSet<>();
		for (MessageConstraint mc : constraints)
			portals.add(mc.getPortal());
		for (Portal<?> portal : portals)
			Portals.setConstraints(portal, constraints);

		return null;
	}

	// TODO: Need to do precise mapping. For the morment just mapping acording to the order. Now just doing round-robin assignment.
	private Map<Integer, List<Set<Worker<?, ?>>>> mapPartitionstoMachines(List<Set<Worker<?, ?>>> partitionList,
			List<Integer> connectedMachineIDs) {

		Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap = new HashMap<Integer, List<Set<Worker<?, ?>>>>();
		int index = 0;
		for (Set<Worker<?, ?>> partition : partitionList) {
			if (index > connectedMachineIDs.size() - 1)
				index = 0;

			if (!partitionsMachineMap.containsKey(connectedMachineIDs.get(index)))
				partitionsMachineMap.put(connectedMachineIDs.get(index), new LinkedList<Set<Worker<?, ?>>>());

			partitionsMachineMap.get(connectedMachineIDs.get(index++)).add(partition);
		}

		return partitionsMachineMap;
	}

	private Configuration makeConfiguration(Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap, Map<Integer, Integer> coreCounts,
			String jarFilePath, String outterClass, String topLevelClass) {

		Configuration.Builder builder = Configuration.builder();

		Identity<Integer> first = new Identity<>(), second = new Identity<>();
		Pipeline<Integer, Integer> pipeline = new Pipeline<>(first, second);
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		pipeline.visit(cwv);

		PartitionParameter.Builder partParam = PartitionParameter.builder("partition", new LinkedList<>(coreCounts.values()));

		// TODO: need to add correct blob factory.
		BlobFactory factory = new Interpreter.InterpreterBlobFactory();
		partParam.addBlobFactory(factory);
				
		for(Integer machineID : partitionsMachineMap.keySet())
		{
			List<Set<Worker<?, ?>>> blobList = partitionsMachineMap.get(machineID);
			for(Set<Worker<?, ?>> blobWorkers : blobList)
			{
				//TODO: One core per blob. Need to change this.
				partParam.addBlob(machineID, 1, factory, blobWorkers);
			}			
		}
		
		builder.addParameter(partParam.build());

		builder.putExtraData("jarFilePath", jarFilePath);
		builder.putExtraData("outterClass", outterClass);
		builder.putExtraData("topLevelClass", topLevelClass);

		return builder.build();
	}

	
}