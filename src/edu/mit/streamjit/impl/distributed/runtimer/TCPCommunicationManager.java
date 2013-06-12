/**
 * @author Sumanan sumanan@mit.edu
 * @since May 13, 2013
 */
package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.mit.streamjit.impl.distributed.api.Request;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.TCPSocket;

public class TCPCommunicationManager implements CommunicationManager {

	private Map<Integer, TCPSocket> socketMap; // (machineID, TCPSocket)
	private int listenPort;

	public TCPCommunicationManager(int listenPort) {
		socketMap = new HashMap<Integer, TCPSocket>();
		this.listenPort = listenPort;
	}

	public TCPCommunicationManager() {
		this(GlobalConstants.PORTNO);
	}

	@Override
	public <T> T readObject(int machineID) throws IOException, ClassNotFoundException {
		if (!socketMap.containsKey(machineID))
			throw new IllegalArgumentException("Invalid machineID. No machine is connected with machineID " + machineID);

		return socketMap.get(machineID).receiveObject();
	}

	@Override
	public void writeObject(int machineID, Object obj) throws IOException {
		if (!socketMap.containsKey(machineID))
			throw new IllegalArgumentException("Invalid machineID. No machine is connected with machineID " + machineID);

		socketMap.get(machineID).sendObject(obj);
	}

	@Override
	public void connectMachines(int noOfmachines) throws IOException {
		ListenerSocket listnerSckt = new ListenerSocket(this.listenPort, noOfmachines);
		listnerSckt.start();
		socketMap.clear();
		int machineID = 1; // controller gets machineID 0.
		while (true) {
			List<TCPSocket> acceptedSocketList = listnerSckt.getAcceptedSockets();
			for (TCPSocket s : acceptedSocketList) {
				socketMap.put(machineID++, s);
				System.out.println("StreamNode connected: " + s.toString());
				if (!(socketMap.size() < noOfmachines))
					break;
			}

			if (!(socketMap.size() < noOfmachines))
				break;

			// Rather than continuously polling the listenersocket, lets wait some time before the next poll.
			try {
				Thread.sleep(1000);
				System.out.println("Waiting for required nodes to be connected. Listener is still listening...");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		listnerSckt.stopListening();
	}

	@Override
	public void connectMachines(long timeOut) throws IOException {
		// TODO: Implement a timer and call the listnerSckt.stopListening();
	}

	@Override
	public void closeAllConnections() throws IOException {
		for (TCPSocket s : socketMap.values()) {
			s.closeConnection();
		}
	}

	@Override
	public void closeConnection(int machineID) throws IOException {
		if (!socketMap.containsKey(machineID))
			throw new IllegalArgumentException("Invalid machineID. No machine is connected with machineID " + machineID);

		socketMap.get(machineID).closeConnection();
	}

	@Override
	public boolean isConnected(int machineID) {

		if (socketMap.containsKey(machineID)) {
			TCPSocket ss = socketMap.get(machineID);

			return ss.isStillconnected();
		}
		return false;
	}

	@Override
	public List<Integer> getConnectedMachineIDs() {

		List<Integer> connectedMachineIDs = new LinkedList<>();

		for (int key : socketMap.keySet()) {

			if (socketMap.get(key).isStillconnected()) {
				connectedMachineIDs.add(key);
			}
		}
		return connectedMachineIDs;
	}
}