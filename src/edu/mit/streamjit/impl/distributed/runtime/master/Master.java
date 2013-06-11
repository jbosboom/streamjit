package edu.mit.streamjit.impl.distributed.runtime.master;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter.BlobSpecifier;
import edu.mit.streamjit.impl.distributed.runtime.api.BlobsManager;
import edu.mit.streamjit.impl.distributed.runtime.api.Command;
import edu.mit.streamjit.impl.distributed.runtime.api.JsonString;
import edu.mit.streamjit.impl.distributed.runtime.api.NodeInfo;
import edu.mit.streamjit.impl.distributed.runtime.api.Request;
import edu.mit.streamjit.impl.distributed.runtime.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.runtime.slave.BlobsManagerImpl;
import edu.mit.streamjit.impl.distributed.runtime.slave.DistributedBlob;
import edu.mit.streamjit.impl.interp.Interpreter;

/**
 * @author Sumanan sumanan@mit.edu
 * @since May 10, 2013
 */
public class Master {

	private CommunicationManager comManager;

	BlobsManager blobsManager;

	private List<Integer> slaveIDs;

	Map<Integer, NodeInfo> nodeInfoMap;

	public Master() {
		this.comManager = new TCPCommunicationManager();
	}

	public void connect(int noOfslaves) {
		// TODO: Need to handle this exception well.
		try {
			comManager.connectMachines(noOfslaves); // Because the noOfnodes includes the master node also
		} catch (IOException e) {
			System.out.println("Connection Error...");
			e.printStackTrace();
			System.exit(0);
		}
		setMachineIds();
		getNodeInfo();
	}

	private void setMachineIds() {
		this.slaveIDs = comManager.getConnectedMachineIDs();
		for (int key : this.slaveIDs) {
			try {
				comManager.writeObject(key, Request.machineID);
				comManager.writeObject(key, new Integer(key));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void getNodeInfo() {
		nodeInfoMap = new HashMap<>();
		nodeInfoMap.put(0, NodeInfo.getMyinfo()); // available cores at master. master's machineID is 0

		sendToAll(Request.NodeInfo);

		for (int key : this.slaveIDs) {
			try {
				NodeInfo nodeinfo = comManager.readObject(key);
				nodeInfoMap.put(key, nodeinfo);
			} catch (IOException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * Blocking call.
	 * 
	 * @return : Map, key is machineID and value is coreCount.
	 */
	public Map<Integer, Integer> getCoreCount() {
		Map<Integer, Integer> coreCounts = new HashMap<>();
		coreCounts.put(0, Runtime.getRuntime().availableProcessors()); // available cores at master. master's machineID is 0

		sendToAll(Request.maxCores);

		for (int key : this.slaveIDs) {
			try {
				Integer count = comManager.readObject(key);
				coreCounts.put(key, count);
			} catch (IOException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return coreCounts;
	}

	public void setPartition(Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap, String toplevelclass,
			List<MessageConstraint> constraints, Worker<?, ?> source, Worker<?, ?> sink) {
		String jarFilePath = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();

		Configuration cfg = makeConfiguration(partitionsMachineMap, jarFilePath, toplevelclass, source, sink);
		JsonString json = new JsonString(cfg.toJson());
		sendToAll(json);

		// TODO: Change this later.
		Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap = (Map<Token, Map.Entry<Integer, Integer>>) cfg
				.getExtraData(GlobalConstants.TOKEN_MACHINE_MAP);
		Map<Token, Integer> portIdMap = (Map<Token, Integer>) cfg.getExtraData(GlobalConstants.PORTID_MAP);

		Map<Integer, NodeInfo> nodeInfoMap = (Map<Integer, NodeInfo>) cfg.getExtraData(GlobalConstants.NODE_INFO_MAP);
		createMasterBlobs(partitionsMachineMap.get(0), tokenMachineMap, portIdMap, nodeInfoMap);
		sendToAll(Command.START);
	}

	private Configuration makeConfiguration(Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap, String jarFilePath,
		String topLevelClass, Worker<?, ?> source, Worker<?, ?> sink) {

		Configuration.Builder builder = Configuration.builder();

		Identity<Integer> first = new Identity<>(), second = new Identity<>();
		Pipeline<Integer, Integer> pipeline = new Pipeline<>(first, second);
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		pipeline.visit(cwv);

		List<Integer> coresPerMachine = new ArrayList<>();
		for (List<Set<Worker<?, ?>>> blobList : partitionsMachineMap.values()) {
			coresPerMachine.add(blobList.size());
		}

		PartitionParameter.Builder partParam = PartitionParameter.builder(GlobalConstants.PARTITION, coresPerMachine);

		// TODO: need to add correct blob factory.
		BlobFactory factory = new Interpreter.InterpreterBlobFactory();
		partParam.addBlobFactory(factory);

		for (Integer machineID : partitionsMachineMap.keySet()) {
			List<Set<Worker<?, ?>>> blobList = partitionsMachineMap.get(machineID);
			for (Set<Worker<?, ?>> blobWorkers : blobList) {
				// TODO: One core per blob. Need to change this.
				partParam.addBlob(machineID, 1, factory, blobWorkers);
			}
		}

		builder.addParameter(partParam.build());

		Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap = new HashMap<>();
		Map<Token, Integer> portIdMap = new HashMap<>();

		buildTokenMap(partitionsMachineMap, tokenMachineMap, portIdMap, source, sink);

		builder.putExtraData(GlobalConstants.JARFILE_PATH, jarFilePath);
		builder.putExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME, topLevelClass);
		builder.putExtraData(GlobalConstants.NODE_INFO_MAP, nodeInfoMap);
		builder.putExtraData(GlobalConstants.TOKEN_MACHINE_MAP, tokenMachineMap);
		builder.putExtraData(GlobalConstants.PORTID_MAP, portIdMap);

		return builder.build();
	}

	// Key - MachineID of the source node, Value - MachineID of the destination node.
	// all maps should be initialized.
	private void buildTokenMap(Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap,
			Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap, Map<Token, Integer> portIdMap, Worker<?, ?> source,
			Worker<?, ?> sink) {

		assert partitionsMachineMap != null : "partitionsMachineMap is null";
		assert tokenMachineMap != null : "tokenMachineMap is null";
		assert portIdMap != null : "portIdMap is null";

		int startPortNo = 24896;
		for (Integer machineID : partitionsMachineMap.keySet()) {
			List<Set<Worker<?, ?>>> blobList = partitionsMachineMap.get(machineID);
			Set<Worker<?, ?>> allWorkers = new HashSet<>(); // Contains all workers those are assigned to the current machineID
															// machine.
			for (Set<Worker<?, ?>> blobWorkers : blobList) {
				allWorkers.addAll(blobWorkers);
			}

			for (Worker<?, ?> w : allWorkers) {
				for (Worker<?, ?> succ : Workers.getSuccessors(w)) {
					if (allWorkers.contains(succ))
						continue;
					int dstMachineID = getAssignedMachine(succ, partitionsMachineMap);
					Token t = new Token(w, succ);
					tokenMachineMap.put(t, new AbstractMap.SimpleEntry<>(machineID, dstMachineID));
					portIdMap.put(t, startPortNo++);
				}
			}
		}

		Token headToken = Token.createOverallInputToken(source);
		int dstMachineID = getAssignedMachine(source, partitionsMachineMap);
		tokenMachineMap.put(headToken, new AbstractMap.SimpleEntry<>(0, dstMachineID));
		portIdMap.put(headToken, startPortNo++);

		Token tailToken = Token.createOverallOutputToken(sink);
		int srcMahineID = getAssignedMachine(sink, partitionsMachineMap);
		tokenMachineMap.put(tailToken, new AbstractMap.SimpleEntry<>(srcMahineID, 0));
		portIdMap.put(tailToken, startPortNo++);
	}

	/**
	 * @param worker
	 * @return the machineID where on which the passed worker is assigned.
	 */
	public int getAssignedMachine(Worker<?, ?> worker, Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap) {
		for (Integer machineID : partitionsMachineMap.keySet()) {
			for (Set<Worker<?, ?>> workers : partitionsMachineMap.get(machineID)) {
				if (workers.contains(worker))
					return machineID;
			}
		}

		throw new IllegalArgumentException(String.format("%s is not assigned to anyof the machines", worker));
	}

	// private void buildTokenMachineMap(ImmutableSet<Worker<?, ?>> workerset, PartitionParameter partParam) {
	// for (Worker<?, ?> w : workerset) {
	// for (Worker<?, ?> pred : Workers.getPredecessors(w)) {
	// if (workerset.contains(pred))
	// continue;
	// int neighbour = partParam.getAssignedMachine(pred);
	// this.tokenMachineMap.put(new Token(pred, w), neighbour);
	// }
	//
	// for (Worker<?, ?> succ : Workers.getSuccessors(w)) {
	// if (workerset.contains(succ))
	// continue;
	// int neighbour = partParam.getAssignedMachine(succ);
	// this.tokenMachineMap.put(new Token(w, succ), neighbour);
	// }
	// }
	// }

	private void sendToAll(Object object) {
		for (int key : this.slaveIDs) {
			try {
				comManager.writeObject(key, object);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void createMasterBlobs(List<Set<Worker<?, ?>>> blobWorkersList, Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap,
			Map<Token, Integer> portIdMap, Map<Integer, NodeInfo> nodeInfoMap) {
		Blob b = new DistributedBlob(blobWorkersList, Collections.<MessageConstraint> emptyList());
		Set<Blob> blobSet = new HashSet<>();
		blobSet.add(b);
		blobsManager = new BlobsManagerImpl(blobSet, tokenMachineMap, portIdMap, nodeInfoMap);
		blobsManager.start();
	}
}
