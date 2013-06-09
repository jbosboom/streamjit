package edu.mit.streamjit.impl.distributed.runtime.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.distributed.runtime.api.BlobsManager;
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

	public void setPartition(Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap, String outerClass, String toplevelclass,
			List<MessageConstraint> constraints) {
		String jarFilePath = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();

		Configuration confg = makeConfiguration(partitionsMachineMap, jarFilePath, outerClass, toplevelclass);
		JsonString json = new JsonString(confg.toJson());
		sendToAll(json);

		createMasterBlobs(partitionsMachineMap.get(0));
	}

	private Configuration makeConfiguration(Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap, String jarFilePath,
			String outterClass, String topLevelClass) {

		Configuration.Builder builder = Configuration.builder();

		Identity<Integer> first = new Identity<>(), second = new Identity<>();
		Pipeline<Integer, Integer> pipeline = new Pipeline<>(first, second);
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		pipeline.visit(cwv);

		List<Integer> coresPerMachine = new ArrayList<>();
		for (List<Set<Worker<?, ?>>> blobList : partitionsMachineMap.values()) {
			coresPerMachine.add(blobList.size());
		}

		PartitionParameter.Builder partParam = PartitionParameter.builder("partition", coresPerMachine);

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

		builder.putExtraData(GlobalConstants.JARFILE_PATH, jarFilePath);
		builder.putExtraData(GlobalConstants.OUTTER_CLASS_NAME, outterClass);
		builder.putExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME, topLevelClass);
		builder.putExtraData(GlobalConstants.NODE_INFO_MAP, nodeInfoMap);

		return builder.build();
	}

	private void sendToAll(Object object) {
		for (int key : this.slaveIDs) {
			try {
				comManager.writeObject(key, object);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void createMasterBlobs(List<Set<Worker<?, ?>>> blobWorkersList) {
		Blob b = new DistributedBlob(blobWorkersList, Collections.<MessageConstraint> emptyList());
		Set<Blob> blobSet = new HashSet<>();
		blobSet.add(b);
		blobsManager = new BlobsManagerImpl(blobSet);
		blobsManager.start();
	}
}
