package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.net.Socket;
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
public class CommunicationManagerImpl implements CommunicationManager {

	private Map<Integer, Connection> connectionMap; // (machineID, TCPConnection)
	private int listenPort;

	public CommunicationManagerImpl(int listenPort) {
		connectionMap = new HashMap<Integer, Connection>();
		this.listenPort = listenPort;
	}

	public CommunicationManagerImpl() {
		this(GlobalConstants.PORTNO);
	}

	@Override
	public <T> T readObject(int machineID) throws IOException, ClassNotFoundException {
		if (!connectionMap.containsKey(machineID))
			throw new IllegalArgumentException("Invalid machineID. No machine is connected with machineID " + machineID);

		return connectionMap.get(machineID).readObject();
	}

	@Override
	public void writeObject(int machineID, Object obj) throws IOException {
		if (!connectionMap.containsKey(machineID))
			throw new IllegalArgumentException("Invalid machineID. No machine is connected with machineID " + machineID);

		connectionMap.get(machineID).writeObject(obj);
	}

	@Override
	public void connectMachines(Map<CommunicationType, Integer> commTypes) throws IOException {
		int totalTcpConnections = 0;
		if (commTypes.containsKey(CommunicationType.TCP))
			totalTcpConnections += commTypes.get(CommunicationType.TCP);

		// TODO: Change this later.
		// For the moment lets communicate with the local StreamNode through TCP port.
		if (commTypes.containsKey(CommunicationType.LOCAL)) {
			totalTcpConnections += 1;
		}

		ListenerSocket listnerSckt = new ListenerSocket(this.listenPort, totalTcpConnections);
		listnerSckt.start();
		if (commTypes.containsKey(CommunicationType.LOCAL))
			createTcpLocalStreamNode();
		connectionMap.clear();
		int nodeID = 0; // nodeID 0 goes to the controller instance. We need this because Controller handles the head and tail
						// channels, though it doesn't executes any workers.
		while (true) {
			List<Socket> acceptedSocketList = listnerSckt.getAcceptedSockets();
			for (Socket s : acceptedSocketList) {
				connectionMap.put(nodeID++, new TCPConnection(s));
				System.out.println("StreamNode connected: " + s.toString());
				if (!(connectionMap.size() < totalTcpConnections))
					break;
			}

			if (!(connectionMap.size() < totalTcpConnections))
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

	/**
	 * Creates JVM local {@link StreamNode}. Only one JVM local {@link StreamNode} can exist.
	 */
	private void createTcpLocalStreamNode() {
		new Thread() {
			public void run() {
				try {
					ConnectionFactory cf = new ConnectionFactory();
					Connection connection = cf.getConnection("127.0.0.1", listenPort);
					StreamNode.getInstance(connection).start();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}.start();
	}

	@Override
	public void connectMachines(long timeOut) throws IOException {
		// TODO: Implement a timer and call the listnerSckt.stopListening();
	}

	@Override
	public void closeAllConnections() throws IOException {
		for (Connection s : connectionMap.values()) {
			s.closeConnection();
		}
	}

	@Override
	public void closeConnection(int machineID) throws IOException {
		if (!connectionMap.containsKey(machineID))
			throw new IllegalArgumentException("Invalid machineID. No machine is connected with machineID " + machineID);

		connectionMap.get(machineID).closeConnection();
	}

	@Override
	public boolean isConnected(int machineID) {

		if (connectionMap.containsKey(machineID)) {
			Connection ss = connectionMap.get(machineID);
			return ss.isStillConnected();
		}
		return false;
	}

	@Override
	public List<Integer> getConnectedMachineIDs() {

		List<Integer> connectedMachineIDs = new LinkedList<>();

		for (int key : connectionMap.keySet()) {

			if (connectionMap.get(key).isStillConnected()) {
				connectedMachineIDs.add(key);
			}
		}
		return connectedMachineIDs;
	}
}