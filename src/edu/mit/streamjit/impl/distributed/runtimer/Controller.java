package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.concurrent.ConcurrentChannelFactory;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationStringProcessor.ConfigType;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.Request;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.CommunicationType;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.StreamNodeAgent;
import edu.mit.streamjit.impl.interp.ChannelFactory;

/**
 * {@link Controller} controls all {@link StreamNode}s in runtime. It has
 * {@link CommunicationManager} and through {@link CommunicationManager}
 * issue/receive commands from {@link StreamNode}s}. </p> TODO: Need to make
 * {@link Controller} running on a separate thread such that it keep tracks of
 * all messages sent/received from {@link StreamNode}s.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 10, 2013
 */
public class Controller {

	TCPConnectionProvider conProvider;

	int startPortNo = 24896; // Just a random magic number.

	private CommunicationManager comManager;

	/**
	 * {@link StreamNodeAgent}s for all connected {@link StreamNode}s mapped
	 * with corresponding nodeID.
	 */
	private Map<Integer, StreamNodeAgent> StreamNodeMap;

	/**
	 * NodeID for the {@link Controller}. We need this as Controller need to
	 * handle the head and tail buffers. Most of the cases ID 0 will be assigned
	 * to the Controller.
	 */
	public final int controllerNodeID;

	private Set<TCPConnectionInfo> currentConInfos;

	public Controller() {
		this.comManager = new BlockingCommunicationManager();
		this.controllerNodeID = 0;
		this.currentConInfos = new HashSet<>();
	}

	/**
	 * Establishes the connections with {@link StreamNode}s.
	 * 
	 * @param comTypeCount
	 *            : A map that tells how many connections are expected to be
	 *            established for each {@link CommunicationType}s
	 */
	public void connect(Map<CommunicationType, Integer> comTypeCount) {
		// TODO: Need to handle this exception well.
		try {
			StreamNodeMap = comManager.connectMachines(comTypeCount);
		} catch (IOException e) {
			System.out.println("Connection Error...");
			e.printStackTrace();
			System.exit(0);
		}

		if (StreamNodeMap.keySet().contains(controllerNodeID))
			throw new AssertionError(
					"Conflict in nodeID assignment. controllerNodeID has been assigned to a SteamNode");

		setMachineIds();
		sendToAll(Request.NodeInfo);
	}

	private void setMachineIds() {
		for (StreamNodeAgent agent : StreamNodeMap.values()) {
			try {
				// TODO: Need to send in a single object.
				agent.writeObject(Request.machineID);
				agent.writeObject(new Integer(agent.getNodeID()));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Blocking call.
	 * 
	 * @return : A map where key is nodeID and value is number of cores in the
	 *         corresponding node.
	 */
	public Map<Integer, Integer> getCoreCount() {
		Map<Integer, Integer> coreCounts = new HashMap<>();

		for (StreamNodeAgent agent : StreamNodeMap.values()) {
			NodeInfo nodeInfo = agent.getNodeInfo();
			Integer count = nodeInfo.getAvailableCores();
			coreCounts.put(agent.getNodeID(), count);
		}
		return coreCounts;
	}

	public void newApp(StreamJitApp app) {
		Configuration.Builder builder = Configuration.builder(app
				.getStaticConfiguration());

		Map<Integer, NodeInfo> nodeInfoMap = new HashMap<>();
		for (StreamNodeAgent agent : StreamNodeMap.values())
			nodeInfoMap.put(agent.getNodeID(), agent.getNodeInfo());

		nodeInfoMap.put(controllerNodeID, NodeInfo.getMyinfo());

		// TODO: Ensure the need of this switch parameter.
		List<ChannelFactory> universe = Arrays
				.<ChannelFactory> asList(new ConcurrentChannelFactory());
		SwitchParameter<ChannelFactory> cfParameter = new SwitchParameter<ChannelFactory>(
				"channelFactory", ChannelFactory.class, universe.get(0),
				universe);

		builder.addParameter(cfParameter).putExtraData(
				GlobalConstants.NODE_INFO_MAP, nodeInfoMap);

		this.conProvider = new TCPConnectionProvider(controllerNodeID,
				nodeInfoMap);

		ConfigurationString json = new ConfigurationString(builder.build()
				.toJson(), ConfigType.STATIC, null);
		sendToAll(json);
	}

	public void reconfigure() {
		Configuration.Builder builder = Configuration.builder(app
				.getDynamicConfiguration());

		Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap = new HashMap<>();
		Map<Token, Integer> portIdMap = new HashMap<>();

		Map<Token, TCPConnectionInfo> conInfoMap = buildConInfoMap(
				app.partitionsMachineMap1, app.source1, app.sink1);

		builder.putExtraData(GlobalConstants.TOKEN_MACHINE_MAP, tokenMachineMap)
				.putExtraData(GlobalConstants.PORTID_MAP, portIdMap);

		builder.putExtraData(GlobalConstants.CONINFOMAP, conInfoMap);

		Configuration cfg = builder.build();
		String jsonStirng = cfg.toJson();

		ImmutableMap<Integer, DrainData> drainDataMap = app.getDrainData();

		for (StreamNodeAgent node : StreamNodeMap.values()) {
			try {
				ConfigurationString json = new ConfigurationString(jsonStirng,
						ConfigType.DYNAMIC, drainDataMap.get(node.getNodeID()));
				node.writeObject(json);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		setupHeadTail1(conInfoMap, app.bufferMap,
				Token.createOverallInputToken(app.source1),
				Token.createOverallOutputToken(app.sink1));

		start();
	}

	/**
	 * Setup the headchannel and tailchannel.
	 * 
	 * @param cfg
	 * @param bufferMap
	 * @param headToken
	 * @param tailToken
	 */
	private void setupHeadTail1(Map<Token, TCPConnectionInfo> conInfoMap,
			ImmutableMap<Token, Buffer> bufferMap, Token headToken,
			Token tailToken) {

		TCPConnectionInfo headconInfo = conInfoMap.get(headToken);
		assert headconInfo != null : "No head connection info exists in conInfoMap";
		assert headconInfo.getSrcID() == controllerNodeID : "Head channel should start from the controller.";

		if (!bufferMap.containsKey(headToken))
			throw new IllegalArgumentException(
					"No head buffer in the passed bufferMap.");

		headChannel = new HeadChannel(bufferMap.get(headToken), conProvider,
				headconInfo, "headChannel - " + headToken.toString(), 0);

		TCPConnectionInfo tailconInfo = conInfoMap.get(tailToken);
		assert tailconInfo != null : "No tail connection info exists in conInfoMap";
		assert tailconInfo.getDstID() == controllerNodeID : "Tail channel should ends at the controller.";

		if (!bufferMap.containsKey(tailToken))
			throw new IllegalArgumentException(
					"No tail buffer in the passed bufferMap.");

		tailChannel = new TailChannel(bufferMap.get(tailToken), conProvider,
				tailconInfo, "tailChannel - " + tailToken.toString(), 0, 10000);
	}

	private Map<Token, TCPConnectionInfo> buildConInfoMap(
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap,
			Worker<?, ?> source, Worker<?, ?> sink) {

		assert partitionsMachineMap != null : "partitionsMachineMap is null";

		Set<TCPConnectionInfo> usedConInfos = new HashSet<>();
		Map<Token, TCPConnectionInfo> conInfoMap = new HashMap<>();

		for (Integer machineID : partitionsMachineMap.keySet()) {
			List<Set<Worker<?, ?>>> blobList = partitionsMachineMap
					.get(machineID);
			Set<Worker<?, ?>> allWorkers = new HashSet<>(); // Contains all
															// workers those are
															// assigned to the
															// current machineID
															// machine.
			for (Set<Worker<?, ?>> blobWorkers : blobList) {
				allWorkers.addAll(blobWorkers);
			}

			for (Worker<?, ?> w : allWorkers) {
				for (Worker<?, ?> succ : Workers.getSuccessors(w)) {
					if (allWorkers.contains(succ))
						continue;
					int dstMachineID = getAssignedMachine(succ,
							partitionsMachineMap);
					Token t = new Token(w, succ);
					addtoconInfoMap(machineID, dstMachineID, t, usedConInfos,
							conInfoMap);
				}
			}
		}

		Token headToken = Token.createOverallInputToken(source);
		int dstMachineID = getAssignedMachine(source, partitionsMachineMap);
		addtoconInfoMap(controllerNodeID, dstMachineID, headToken,
				usedConInfos, conInfoMap);

		Token tailToken = Token.createOverallOutputToken(sink);
		int srcMahineID = getAssignedMachine(sink, partitionsMachineMap);
		addtoconInfoMap(srcMahineID, controllerNodeID, tailToken, usedConInfos,
				conInfoMap);

		return conInfoMap;
	}

	/**
	 * Just extracted from {@link #buildConInfoMap(Map, Worker, Worker)} because
	 * the code snippet in this method happened to repeat three times inside the
	 * {@link #buildConInfoMap(Map, Worker, Worker)} method.
	 */
	private void addtoconInfoMap(int srcID, int dstID, Token t,
			Set<TCPConnectionInfo> usedConInfos,
			Map<Token, TCPConnectionInfo> conInfoMap) {

		ConnectionInfo conInfo = new ConnectionInfo(srcID, dstID);

		List<TCPConnectionInfo> conSet = getTcpConInfo(conInfo);
		TCPConnectionInfo tcpConInfo = null;

		for (TCPConnectionInfo con : conSet) {
			if (!usedConInfos.contains(con)) {
				tcpConInfo = con;
				break;
			}
		}

		if (tcpConInfo == null) {
			tcpConInfo = new TCPConnectionInfo(srcID, dstID, startPortNo++);
			this.currentConInfos.add(tcpConInfo);
		}

		conInfoMap.put(t, tcpConInfo);
		usedConInfos.add(tcpConInfo);
	}

	private List<TCPConnectionInfo> getTcpConInfo(ConnectionInfo conInfo) {
		List<TCPConnectionInfo> conList = new ArrayList<>();
		for (TCPConnectionInfo tcpconInfo : currentConInfos) {
			if (conInfo.equals(tcpconInfo))
				conList.add(tcpconInfo);
		}
		return conList;
	}

	public Set<Integer> getAllNodeIDs() {
		return StreamNodeMap.keySet();
	}

	public void send(int nodeID, CTRLRMessageElement message) {
		assert StreamNodeMap.containsKey(nodeID) : String.format(
				"No StreamNode with nodeID %d found", nodeID);
		StreamNodeAgent agent = StreamNodeMap.get(nodeID);
		try {
			agent.writeObject(message);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param worker
	 * @return the machineID where on which the passed worker is assigned.
	 */
	private int getAssignedMachine(Worker<?, ?> worker,
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap) {
		for (Integer machineID : partitionsMachineMap.keySet()) {
			for (Set<Worker<?, ?>> workers : partitionsMachineMap
					.get(machineID)) {
				if (workers.contains(worker))
					return machineID;
			}
		}

		throw new IllegalArgumentException(String.format(
				"%s is not assigned to anyof the machines", worker));
	}

	public void sendToAll(Object object) {
		for (StreamNodeAgent node : StreamNodeMap.values()) {
			try {
				node.writeObject(object);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public TCPConnectionProvider getConProvider() {
		return conProvider;
	}

	public void closeAll() {
		try {
			comManager.closeAllConnections();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// TODO: Temporary fix. Need to come up with a better solution to to set
	// DrainProcessor to StreamnodeAgent's messagevisitor.
	public void setDrainProcessor(SNDrainProcessor dp) {
		for (StreamNodeAgent agent : StreamNodeMap.values()) {
			agent.setDrainProcessor(dp);
		}
	}
}
