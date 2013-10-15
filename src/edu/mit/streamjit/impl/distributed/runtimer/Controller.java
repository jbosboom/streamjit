package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ImmutableMap;

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
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.concurrent.ConcurrentChannelFactory;
import edu.mit.streamjit.impl.distributed.StreamJitApp;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;
import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryOutputChannel;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationStringProcessor.ConfigType;
import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.DrainElement;
import edu.mit.streamjit.impl.distributed.common.DrainElement.DrainProcessor;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.Request;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
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

	TCPConnectionProvider conProvider;

	int reconf = 0;

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
	private TailChannel tailChannel;

	private Set<TCPConnectionInfo> currentConInfos;

	private Thread headThread;
	private Thread tailThread;

	StreamJitApp app;

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
			tailChannel.reset();
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
				mergedConfig.toJson(), ConfigType.DYNAMIC);
		sendToAll(json);

		setupHeadTail(cfg, bufferMap, Token.createOverallInputToken(source),
				Token.createOverallOutputToken(sink));
	}

	public void newApp(StreamJitApp app) {
		this.app = app;

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
				.toJson(), ConfigType.STATIC);
		sendToAll(json);
	}

	public void reconfigure() {
		reconf++;
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
		ConfigurationString json = new ConfigurationString(cfg.toJson(),
				ConfigType.DYNAMIC);
		sendToAll(json);

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

		if (headconInfo.getDstID() != controllerNodeID) {
			if (!bufferMap.containsKey(headToken))
				throw new IllegalArgumentException(
						"No head buffer in the passed bufferMap.");

			headChannel = new TCPOutputChannel(bufferMap.get(headToken),
					conProvider, headconInfo, "headChannel - "
							+ headToken.toString(), false);
		}

		TCPConnectionInfo tailconInfo = conInfoMap.get(tailToken);
		assert tailconInfo != null : "No tail connection info exists in conInfoMap";
		assert tailconInfo.getDstID() == controllerNodeID : "Tail channel should ends at the controller.";

		if (tailconInfo.getSrcID() != controllerNodeID) {
			if (!bufferMap.containsKey(tailToken))
				throw new IllegalArgumentException(
						"No tail buffer in the passed bufferMap.");

			tailChannel = new TailChannel(bufferMap.get(tailToken),
					conProvider, tailconInfo, "tailChannel - "
							+ tailToken.toString(), false, 10000);
		}
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

			// headChannel = new TCPOutputChannel(bufferMap.get(headToken),
			// portIdMap.get(headToken), "headChannel - "
			// + headToken.toString(), false);
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
			//
			// tailChannel = new TailChannel(bufferMap.get(tailToken),
			// ipAddress,
			// portIdMap.get(tailToken), "tailChannel - "
			// + tailToken.toString(), false, 10000);
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

		app.blobtoMachineMap = new HashMap<>();

		for (Integer machineID : partitionsMachineMap.keySet()) {
			List<Set<Worker<?, ?>>> blobList = partitionsMachineMap
					.get(machineID);
			for (Set<Worker<?, ?>> blobWorkers : blobList) {
				// TODO: One core per blob. Need to change this.
				partParam.addBlob(machineID, 1, factory, blobWorkers);

				// TODO: Temp fix to build.
				Token t = Utils.getblobID(blobWorkers);
				app.blobtoMachineMap.put(t, machineID);
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

		TCPConnectionInfo tcpConInfo = getTcpConInfo(conInfo);
		if (tcpConInfo == null || usedConInfos.contains(tcpConInfo)) {
			tcpConInfo = new TCPConnectionInfo(srcID, dstID, startPortNo++);
			this.currentConInfos.add(tcpConInfo);
		}

		conInfoMap.put(t, tcpConInfo);
		usedConInfos.add(tcpConInfo);
	}

	private TCPConnectionInfo getTcpConInfo(ConnectionInfo conInfo) {
		for (TCPConnectionInfo tcpconInfo : currentConInfos) {
			if (conInfo.equals(tcpconInfo))
				return tcpconInfo;
		}
		return null;
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

		int startPortNo = 24896 + (reconf % 10) + 500; // Just a random magic
														// number.

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

	public void drain(Token blobID, boolean isFinal) {
		if (!drainStarted) {
			if (headChannel != null) {
				headChannel.stop(isFinal);
				try {
					headThread.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			drainStarted = true;
		}

		if (!app.blobtoMachineMap.containsKey(blobID))
			throw new IllegalArgumentException(blobID
					+ " not found in the blobtoMachineMap");
		int machineID = app.blobtoMachineMap.get(blobID);
		StreamNodeAgent agent = StreamNodeMap.get(machineID);
		try {
			agent.writeObject(new DrainElement.DoDrain(blobID, !isFinal));
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

	public void drainingFinished(boolean isFinal) {
		System.out.println("Controller : Draining Finished...");
		drainStarted = false;
		if (tailChannel != null) {
			tailChannel.stop();
			try {
				tailThread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (isFinal) {
			try {
				comManager.closeAllConnections();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void awaitForFixInput() throws InterruptedException {
		tailChannel.awaitForFixInput();
	}

	/**
	 * TODO: Temp fix. Change it later.
	 */
	private class TailChannel extends TCPInputChannel {

		int limit;

		int count;

		CountDownLatch latch;

		public TailChannel(Buffer buffer, TCPConnectionProvider conProvider,
				TCPConnectionInfo conInfo, String bufferTokenName,
				Boolean debugPrint, int limit) {
			super(buffer, conProvider, conInfo, bufferTokenName, debugPrint);
			this.limit = limit;
			count = 0;
			latch = new CountDownLatch(1);
		}

		@Override
		public void receiveData() {
			super.receiveData();
			count++;
			if (count == limit)
				latch.countDown();
		}

		private void awaitForFixInput() throws InterruptedException {
			latch.await();
		}

		private void reset() {
			latch = new CountDownLatch(1);
			count = 0;
		}
	}
}
