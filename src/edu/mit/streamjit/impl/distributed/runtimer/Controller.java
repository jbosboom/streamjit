package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.IOInfo;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.concurrent.ConcurrentChannelFactory;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.DrainElement;
import edu.mit.streamjit.impl.distributed.common.DrainElement.DrainProcessor;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.Request;
import edu.mit.streamjit.impl.distributed.common.Utils;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.node.TCPInputChannel;
import edu.mit.streamjit.impl.distributed.node.TCPOutputChannel;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.CommunicationType;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.StreamNodeAgent;
import edu.mit.streamjit.impl.interp.ChannelFactory;
import edu.mit.streamjit.impl.interp.Interpreter;

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

	CommunicationManager comManager;

	/**
	 * {@link StreamNodeAgent}s for all connected {@link StreamNode}s mapped
	 * with corresponding nodeID.
	 */
	private Map<Integer, StreamNodeAgent> StreamNodeMap;

	/**
	 * Keeps track of assigned machine Ids of each blob. This information is
	 * need for draining. TODO: If possible use a better solution.
	 */
	private Map<Token, Integer> blobtoMachineMap;

	/**
	 * NodeID for the {@link Controller}. We need this as Controller need to
	 * handle the head and tail buffers. Most of the cases ID 0 will be assigned
	 * to the Controller.
	 */
	private final int controllerNodeID;

	/**
	 * A {@link BoundaryOutputChannel} for the head of the stream graph. If the
	 * first {@link Worker} happened to fall outside the {@link Controller}, we
	 * need to push the {@link CompiledStream}.offer() data to the first
	 * {@link Worker} of the streamgraph.
	 */
	private BoundaryOutputChannel headChannel;

	/**
	 * A {@link BoundaryInputChannel} for the tail of the whole stream graph. If
	 * the sink {@link Worker} happened to fall outside the {@link Controller},
	 * we need to pull the sink's output in to the {@link Controller} in order
	 * to make {@link CompiledStream} .pull() to work.
	 */
	private BoundaryInputChannel tailChannel;

	private Thread headThread;
	private Thread tailThread;

	public Controller() {
		this.comManager = new BlockingCommunicationManager();
		this.controllerNodeID = 0;
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
	 * Start the execution of the StreamJit application.
	 */
	public void start() {
		if (headChannel != null) {
			headThread = new Thread(headChannel.getRunnable(),
					headChannel.name());
			headThread.start();
		}

		sendToAll(Command.START);

		if (tailChannel != null) {
			tailThread = new Thread(tailChannel.getRunnable(),
					tailChannel.name());
			tailThread.start();
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

	/**
	 * Setup a streamJit application on all connected {@link StreamNode}s.
	 * {@link StreamCompiler} or a tuner can get use of this function to deploy
	 * an application on the connected stream nodes.
	 * 
	 * @param partitionsMachineMap
	 *            Map that contains NodeID and list of blob workers (set of
	 *            workers) mapped to that particular streamnode.
	 * @param jarFilePath
	 *            path of the streamJit application that streamNodes can find
	 *            it. All streamNodes should be able to access the jar file of
	 *            the streamJit application through this path.
	 * @param toplevelclass
	 *            name of the top level stream class. {@link OneToOneElement}
	 *            that has been asked to compile. Should be an unique
	 *            OneToOneElement. Default subtypes of the
	 *            {@link OneToOneElement}s such as {@link Pipeline},
	 *            {@link Splitjoin} and etc shouldn't be passed.
	 * @param constraints
	 * @param source
	 * @param sink
	 * @param bufferMap
	 *            buffers for head and tail channels.
	 */
	public void setPartition(
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap,
			String jarFilePath, String toplevelclass,
			List<MessageConstraint> constraints, Worker<?, ?> source,
			Worker<?, ?> sink, ImmutableMap<Token, Buffer> bufferMap,
			Configuration blobConfigs) {

		Configuration cfg = makeConfiguration(partitionsMachineMap,
				jarFilePath, toplevelclass, source, sink);

		Configuration mergedConfig;
		if (blobConfigs != null) {
			Configuration.Builder builder = Configuration.builder(cfg);

			builder.addSubconfiguration("blobConfigs", blobConfigs);

			mergedConfig = builder.build();
		} else
			mergedConfig = cfg;

		ConfigurationString json = new ConfigurationString(
				mergedConfig.toJson());
		sendToAll(json);

		setupHeadTail(cfg, bufferMap, Token.createOverallInputToken(source),
				Token.createOverallOutputToken(sink));
	}

	/**
	 * Setup the headchannel and tailchannel.
	 * 
	 * @param cfg
	 * @param bufferMap
	 * @param headToken
	 * @param tailToken
	 */
	@SuppressWarnings("unchecked")
	private void setupHeadTail(Configuration cfg,
			ImmutableMap<Token, Buffer> bufferMap, Token headToken,
			Token tailToken) {
		Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap = (Map<Token, Map.Entry<Integer, Integer>>) cfg
				.getExtraData(GlobalConstants.TOKEN_MACHINE_MAP);

		Map<Token, Integer> portIdMap = (Map<Token, Integer>) cfg
				.getExtraData(GlobalConstants.PORTID_MAP);

		Map<Integer, NodeInfo> nodeInfoMap = (Map<Integer, NodeInfo>) cfg
				.getExtraData(GlobalConstants.NODE_INFO_MAP);

		Map.Entry<Integer, Integer> headentry = tokenMachineMap.get(headToken);
		assert headentry != null : "No head token exists in tokenMachineMap";
		assert headentry.getKey() == controllerNodeID : "Head channel should start from the controller.";

		if (headentry.getValue() != controllerNodeID) {
			if (!bufferMap.containsKey(headToken))
				throw new IllegalArgumentException(
						"No head buffer in the passed bufferMap.");

			headChannel = new TCPOutputChannel(bufferMap.get(headToken),
					portIdMap.get(headToken), "headChannel - "
							+ headToken.toString(), false);
		}

		Map.Entry<Integer, Integer> tailentry = tokenMachineMap.get(tailToken);
		assert tailentry != null : "No tail token exists in tokenMachineMap";
		assert tailentry.getValue() == controllerNodeID : "Tail channel should ends at the controller.";

		if (tailentry.getKey() != controllerNodeID) {
			if (!bufferMap.containsKey(tailToken))
				throw new IllegalArgumentException(
						"No tail buffer in the passed bufferMap.");

			int nodeID = tokenMachineMap.get(tailToken).getKey();
			NodeInfo nodeInfo = nodeInfoMap.get(nodeID);
			String ipAddress = nodeInfo.getIpAddress().getHostAddress();

			tailChannel = new TCPInputChannel(bufferMap.get(tailToken),
					ipAddress, portIdMap.get(tailToken), "tailChannel - "
							+ tailToken.toString(), false);
		}
	}

	private Configuration makeConfiguration(
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap,
			String jarFilePath, String topLevelClass, Worker<?, ?> source,
			Worker<?, ?> sink) {

		Map<Integer, Integer> coresPerMachine = new HashMap<>();
		for (Entry<Integer, List<Set<Worker<?, ?>>>> machine : partitionsMachineMap
				.entrySet()) {
			coresPerMachine.put(machine.getKey(), machine.getValue().size());
		}

		PartitionParameter.Builder partParam = PartitionParameter.builder(
				GlobalConstants.PARTITION, coresPerMachine);

		BlobFactory factory = new Interpreter.InterpreterBlobFactory();
		partParam.addBlobFactory(factory);

		blobtoMachineMap = new HashMap<>();

		for (Integer machineID : partitionsMachineMap.keySet()) {
			List<Set<Worker<?, ?>>> blobList = partitionsMachineMap
					.get(machineID);
			for (Set<Worker<?, ?>> blobWorkers : blobList) {
				// TODO: One core per blob. Need to change this.
				partParam.addBlob(machineID, 1, factory, blobWorkers);

				// TODO: Temp fix to build.
				Token t = Utils.getblobID(blobWorkers);
				blobtoMachineMap.put(t, machineID);
			}
		}

		Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap = new HashMap<>();
		Map<Token, Integer> portIdMap = new HashMap<>();

		buildTokenMap(partitionsMachineMap, tokenMachineMap, portIdMap, source,
				sink);

		List<ChannelFactory> universe = Arrays
				.<ChannelFactory> asList(new ConcurrentChannelFactory());
		SwitchParameter<ChannelFactory> cfParameter = new SwitchParameter<ChannelFactory>(
				"channelFactory", ChannelFactory.class, universe.get(0),
				universe);

		Map<Integer, NodeInfo> nodeInfoMap = new HashMap<>();
		for (StreamNodeAgent agent : StreamNodeMap.values())
			nodeInfoMap.put(agent.getNodeID(), agent.getNodeInfo());

		nodeInfoMap.put(controllerNodeID, NodeInfo.getMyinfo());

		Configuration.Builder builder = Configuration.builder();
		builder.addParameter(partParam.build())
				.addParameter(cfParameter)
				.putExtraData(GlobalConstants.JARFILE_PATH, jarFilePath)
				.putExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME,
						topLevelClass)
				.putExtraData(GlobalConstants.NODE_INFO_MAP, nodeInfoMap)
				.putExtraData(GlobalConstants.TOKEN_MACHINE_MAP,
						tokenMachineMap)
				.putExtraData(GlobalConstants.PORTID_MAP, portIdMap);

		return builder.build();
	}

	// Key - MachineID of the source node, Value - MachineID of the destination
	// node.
	// all maps should be initialized.
	private void buildTokenMap(
			Map<Integer, List<Set<Worker<?, ?>>>> partitionsMachineMap,
			Map<Token, Map.Entry<Integer, Integer>> tokenMachineMap,
			Map<Token, Integer> portIdMap, Worker<?, ?> source,
			Worker<?, ?> sink) {

		assert partitionsMachineMap != null : "partitionsMachineMap is null";
		assert tokenMachineMap != null : "tokenMachineMap is null";
		assert portIdMap != null : "portIdMap is null";

		int startPortNo = 24896; // Just a random magic number.
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
					tokenMachineMap.put(t, new AbstractMap.SimpleEntry<>(
							machineID, dstMachineID));
					portIdMap.put(t, startPortNo++);
				}
			}
		}

		Token headToken = Token.createOverallInputToken(source);
		int dstMachineID = getAssignedMachine(source, partitionsMachineMap);
		tokenMachineMap.put(headToken, new AbstractMap.SimpleEntry<>(
				controllerNodeID, dstMachineID));
		portIdMap.put(headToken, startPortNo++);

		Token tailToken = Token.createOverallOutputToken(sink);
		int srcMahineID = getAssignedMachine(sink, partitionsMachineMap);
		tokenMachineMap.put(tailToken, new AbstractMap.SimpleEntry<>(
				srcMahineID, controllerNodeID));
		portIdMap.put(tailToken, startPortNo++);
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

	private void sendToAll(Object object) {
		for (StreamNodeAgent node : StreamNodeMap.values()) {
			try {
				node.writeObject(object);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// Used to identify the first draining call.
	private boolean drainStarted = false;

	public void drain(Token blobID) {
		if (!drainStarted) {
			if (headChannel != null) {
				headChannel.stop();
				try {
					headThread.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			drainStarted = true;
		}

		if (!blobtoMachineMap.containsKey(blobID))
			throw new IllegalArgumentException(blobID
					+ " not found in the blobtoMachineMap");
		int machineID = blobtoMachineMap.get(blobID);
		StreamNodeAgent agent = StreamNodeMap.get(machineID);
		try {
			agent.writeObject(new DrainElement.DoDrain(blobID, false));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// TODO: Temporary fix. Need to come up with a better solution to to set
	// DrainProcessor to StreamnodeAgent's messagevisitor.
	public void setDrainProcessor(DrainProcessor dp) {
		for (StreamNodeAgent agent : StreamNodeMap.values()) {
			agent.setDrainProcessor(dp);
		}
	}

	public void drainingFinished() {
		System.out.println("Controller : Draining Finished...");
		if (tailChannel != null) {
			tailChannel.stop();
			try {
				tailThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		try {
			comManager.closeAllConnections();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
