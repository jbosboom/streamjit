package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.mit.streamjit.impl.distributed.common.ConnectionFactory;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.TCPConnection;
import edu.mit.streamjit.impl.distributed.node.Connection;
import edu.mit.streamjit.impl.distributed.node.StreamNode;

/**
 * @author Sumanan sumanan@mit.edu
 * @since May 13, 2013
 */
public class TCPCommunicationManager implements CommunicationManager {

	private Map<Integer, TCPConnection> socketMap; // (machineID, TCPConnection)
	private int listenPort;

	public TCPCommunicationManager(int listenPort) {
		socketMap = new HashMap<Integer, TCPConnection>();
		this.listenPort = listenPort;
	}

	public TCPCommunicationManager() {
		this(GlobalConstants.PORTNO);
	}

	@Override
	public <T> T readObject(int machineID) throws IOException, ClassNotFoundException {
		if (!socketMap.containsKey(machineID))
			throw new IllegalArgumentException("Invalid machineID. No machine is connected with machineID " + machineID);

		return socketMap.get(machineID).readObject();
	}

	@Override
	public void writeObject(int machineID, Object obj) throws IOException {
		if (!socketMap.containsKey(machineID))
			throw new IllegalArgumentException("Invalid machineID. No machine is connected with machineID " + machineID);

		socketMap.get(machineID).writeObject(obj);
	}

	@Override
	public void connectMachines(Map<CommunicationType, Integer> commTypes) throws IOException {
		int totalTcpConnections = 0;
		int localTcpConnections = 0;
		if (commTypes.containsKey(CommunicationType.TCP))
			totalTcpConnections += commTypes.get(CommunicationType.TCP);

		if (commTypes.containsKey(CommunicationType.TCPLOCAL)) {
			localTcpConnections = commTypes.get(CommunicationType.TCPLOCAL);
			totalTcpConnections += localTcpConnections;
		}

		ListenerSocket listnerSckt = new ListenerSocket(this.listenPort, totalTcpConnections);
		listnerSckt.start();
		createTcpLocalStreamNodes(localTcpConnections);
		socketMap.clear();
		int machineID = 0;
		while (true) {
			List<TCPConnection> acceptedSocketList = listnerSckt.getAcceptedSockets();
			for (TCPConnection s : acceptedSocketList) {
				socketMap.put(machineID++, s);
				System.out.println("StreamNode connected: " + s.toString());
				if (!(socketMap.size() < totalTcpConnections))
					break;
			}

			if (!(socketMap.size() < totalTcpConnections))
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

	private void createTcpLocalStreamNodes(int count) {
		ConnectionFactory cf = new ConnectionFactory();
		for (int i = 0; i < count; i++) {
			try {
				Connection connection = cf.getConnection("127.0.0.1", this.listenPort);
				new StreamNode(connection).start();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void connectMachines(long timeOut) throws IOException {
		// TODO: Implement a timer and call the listnerSckt.stopListening();
	}

	@Override
	public void closeAllConnections() throws IOException {
		for (TCPConnection s : socketMap.values()) {
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
			TCPConnection ss = socketMap.get(machineID);

			return ss.isStillConnected();
		}
		return false;
	}

	@Override
	public List<Integer> getConnectedMachineIDs() {

		List<Integer> connectedMachineIDs = new LinkedList<>();

		for (int key : socketMap.keySet()) {

			if (socketMap.get(key).isStillConnected()) {
				connectedMachineIDs.add(key);
			}
		}
		return connectedMachineIDs;
	}
}