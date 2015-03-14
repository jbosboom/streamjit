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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationProcessor.ConfigType;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.NetworkInfo;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.Request;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.CommunicationType;

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

	private ConnectionProvider conProvider;

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
		builder.putExtraData(GlobalConstants.INETADDRESS_MAP, inetMap);
		NetworkInfo networkinfo = new NetworkInfo(inetMap);
		this.conProvider = new ConnectionProvider(controllerNodeID,
				networkinfo);
		ConfigurationString json = new ConfigurationString(builder.build()
				.toJson(), ConfigType.STATIC, null);
		sendToAll(json);
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

	public void sendToAll(Object object) {
		for (StreamNodeAgent node : StreamNodeMap.values()) {
			try {
				node.writeObject(object);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public ConnectionProvider getConProvider() {
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
}
