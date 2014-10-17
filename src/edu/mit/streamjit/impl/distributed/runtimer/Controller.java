/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.net.InetAddress;
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
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationStringProcessor.ConfigType;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.Request;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.CommunicationType;
import edu.mit.streamjit.impl.distributed.runtimer.StreamNodeAgent;
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

	private TCPConnectionProvider conProvider;

	private int startPortNo = 24896; // Just a random magic number.

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

	public void newApp(Configuration staticCfg) {
		Configuration.Builder builder = Configuration.builder(staticCfg);

		Map<Integer, InetAddress> inetMap = new HashMap<>();
		for (StreamNodeAgent agent : StreamNodeMap.values())
			inetMap.put(agent.getNodeID(), agent.getAddress());

		inetMap.put(controllerNodeID, comManager.getLocalAddress());

		// TODO: Ensure the need of this switch parameter.
		List<ChannelFactory> universe = Arrays
				.<ChannelFactory> asList(new ConcurrentChannelFactory());
		SwitchParameter<ChannelFactory> cfParameter = new SwitchParameter<ChannelFactory>(
				"channelFactory", ChannelFactory.class, universe.get(0),
				universe);

		builder.addParameter(cfParameter).putExtraData(
				GlobalConstants.INETADDRESS_MAP, inetMap);

		this.conProvider = new TCPConnectionProvider(controllerNodeID, inetMap);

		ConfigurationString json = new ConfigurationString(builder.build()
				.toJson(), ConfigType.STATIC, null);
		sendToAll(json);
	}

	public Map<Token, TCPConnectionInfo> buildConInfoMap(
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

	public void registerManager(StreamJitAppManager manager) {
		for (StreamNodeAgent node : StreamNodeMap.values()) {
			node.registerManager(manager);
		}
	}

	public TCPConnectionInfo getNewTCPConInfo(TCPConnectionInfo conInfo) {
		if (currentConInfos.contains(conInfo))
			currentConInfos.remove(conInfo);
		TCPConnectionInfo newConinfo = new TCPConnectionInfo(
				conInfo.getSrcID(), conInfo.getDstID(), startPortNo++);
		currentConInfos.add(newConinfo);

		return newConinfo;
	}
}
