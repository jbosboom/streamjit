/**
 * @author Sumanan sumanan@mit.edu
 * @since May 13, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.master;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.mit.streamjit.impl.distributed.runtime.api.Request;
import edu.mit.streamjit.impl.distributed.runtime.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.runtime.common.TCPSocket;

public class TCPCommunicationManager implements CommunicationManager {

	private Map<Integer, TCPSocket> socketMap;
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
	public boolean writeObject(int machineID, Object obj) throws IOException {
		if (!socketMap.containsKey(machineID))
			throw new IllegalArgumentException("Invalid machineID. No machine is connected with machineID " + machineID);

		socketMap.get(machineID).sendObject(obj);
		return true;
	}

	@Override
	public boolean connectMachines(int noOfmachines) throws IOException {
		ListenerSocket listnerSckt = new ListenerSocket(this.listenPort, noOfmachines);
		listnerSckt.start();
		socketMap.clear();
		int machineID = 0;
		while (true) {
			List<TCPSocket> acceptedSocketList = listnerSckt.getAcceptedSockets();
			for (TCPSocket s : acceptedSocketList) {
				socketMap.put(machineID++, s);

				if (!(socketMap.size() < noOfmachines))
					break;
			}

			if (!(socketMap.size() < noOfmachines))
				break;

			// Rather than continuously polling the listenersocket, lets wait some time before the next poll.
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		listnerSckt.stopListening();
		return true;
	}

	@Override
	public boolean connectMachines(long timeOut) throws IOException {
		// TODO: Implement a timer and call the listnerSckt.stopListening();
		return false;
	}

	@Override
	public boolean closeAllConnections() throws IOException {

		for (TCPSocket s : socketMap.values()) {
			s.closeConnection();
		}
		return true;
	}

	@Override
	public boolean closeConnection(int machineID) throws IOException {
		if (!socketMap.containsKey(machineID))
			throw new IllegalArgumentException("Invalid machineID. No machine is connected with machineID " + machineID);

		socketMap.get(machineID).closeConnection();
		return true;
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

		for (int i = 0; i < socketMap.size(); i++) {

			if (socketMap.get(i).isStillconnected()) {
				connectedMachineIDs.add(i);
			}
		}
		return connectedMachineIDs;
	}

	@Override
	public Map<Integer, Integer> getCoreCount() {
		for (TCPSocket socket : socketMap.values()) {
			try {
				socket.sendObject(Request.maxCores);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		for (TCPSocket socket : socketMap.values()) {
			try {
				socket.receiveObject();
			} catch (IOException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}
}