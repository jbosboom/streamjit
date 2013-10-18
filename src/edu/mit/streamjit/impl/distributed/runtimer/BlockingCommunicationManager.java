package edu.mit.streamjit.impl.distributed.runtimer;

import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.ConnectionFactory;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.MessageElement;
import edu.mit.streamjit.impl.distributed.common.TCPConnection;
import edu.mit.streamjit.impl.distributed.node.StreamNode;

/**
 * {@link CommunicationManager} that uses blocking java.io package. Since this
 * is blocking IO, it runs each connection with a {@link StreamNode} on separate
 * thread.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 13, 2013
 */
public class BlockingCommunicationManager implements CommunicationManager {

	private ImmutableMap<Integer, StreamNodeAgent> SNAgentMap; // (machineID,
	// StreamNodeAgent)
	private int listenPort;

	private Set<SNAgentRunner> SNRunners;

	public BlockingCommunicationManager(int listenPort) {
		this.listenPort = listenPort;
		SNRunners = new HashSet<>();
	}

	public BlockingCommunicationManager() {
		this(GlobalConstants.PORTNO);
	}

	@Override
	public Map<Integer, StreamNodeAgent> connectMachines(
			Map<CommunicationType, Integer> commTypes) throws IOException {
		int totalTcpConnections = 0;
		if (commTypes.containsKey(CommunicationType.TCP))
			totalTcpConnections += commTypes.get(CommunicationType.TCP);

		// TODO: Change this later. We can use Java NIO direct buffer for faster
		// communication. For the moment lets communicate with the local
		// StreamNode through TCP port.
		if (commTypes.containsKey(CommunicationType.LOCAL)) {
			totalTcpConnections += 1;
		}

		ListenerSocket listnerSckt = new ListenerSocket(this.listenPort,
				totalTcpConnections);
		listnerSckt.start();
		if (commTypes.containsKey(CommunicationType.LOCAL))
			createTcpLocalStreamNode();
		ImmutableMap.Builder<Integer, StreamNodeAgent> SNAgentMapbuilder = new ImmutableMap.Builder<>();
		int nodeID = 1; // nodeID 0 goes to the controller instance. We need
						// this, though it doesn't executes any workers,
						// Controller
						// handles the head and tail channels.
		int establishedConnection = 0;
		while (true) {
			List<Socket> acceptedSocketList = listnerSckt.getAcceptedSockets();
			for (Socket s : acceptedSocketList) {
				Connection connection = new TCPConnection(s);
				StreamNodeAgent snAgent = new StreamNodeAgentImpl(nodeID++,
						connection);
				SNAgentMapbuilder.put(snAgent.getNodeID(), snAgent);
				SNAgentRunner runner = new SNAgentRunner(snAgent, connection);
				runner.start();
				SNRunners.add(runner);
				System.out.println("StreamNode connected: " + s.toString());
				establishedConnection++;
				if (!(establishedConnection < totalTcpConnections))
					break;
			}

			if (!(establishedConnection < totalTcpConnections))
				break;

			// Rather than continuously polling the listenersocket, lets wait
			// some time before the next poll.
			try {
				Thread.sleep(1000);
				System.out
						.println("Waiting for required nodes to be connected. Listener is still listening...");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		listnerSckt.stopListening();
		this.SNAgentMap = SNAgentMapbuilder.build();
		return SNAgentMap;
	}

	/**
	 * Creates JVM local {@link StreamNode}. Only one JVM local
	 * {@link StreamNode} can exist.
	 */
	private void createTcpLocalStreamNode() {
		new Thread() {
			public void run() {
				try {
					Connection connection = ConnectionFactory.getConnection(
							"127.0.0.1", listenPort, true);
					StreamNode.getInstance(connection).start();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}.start();
	}

	@Override
	public void closeAllConnections() throws IOException {
		for (SNAgentRunner runner : SNRunners) {
			runner.close();
		}
	}

	/**
	 * {@link StreamNodeAgent} for blocking IO context.
	 * 
	 * @author Sumanan
	 * 
	 */
	private static class StreamNodeAgentImpl extends StreamNodeAgent {
		Connection connection;

		private StreamNodeAgentImpl(int machineID, Connection connection) {
			super(machineID);
			this.connection = connection;
		}

		@Override
		public void writeObject(Object obj) throws IOException {
			connection.writeObject(obj);
		}

		@Override
		public boolean isConnected() {
			return connection.isStillConnected();
		}
	}

	/**
	 * IO thread that runs a {@link StreamNodeAgent}. Since this is blocking IO
	 * context, each {@link StreamNodeAgent} agent will be running on individual
	 * threaed.
	 * 
	 */
	private static class SNAgentRunner extends Thread {
		StreamNodeAgent SNAgent;
		Connection connection;

		private SNAgentRunner(StreamNodeAgent SNAgent, Connection connection) {
			super(String.format("SNAgentRunner - %d", SNAgent.getNodeID()));
			this.SNAgent = SNAgent;
			this.connection = connection;
		}

		public void run() {
			while (!SNAgent.isStopRequested() && connection.isStillConnected()) {
				try {
					MessageElement me = connection.readObject();
					me.accept(SNAgent.getMv());
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					if (!SNAgent.isStopRequested())
						e.printStackTrace();
				}
			}
		}

		public void close() {
			try {
				SNAgent.stopRequest();
				connection.writeObject(Command.EXIT);
				connection.closeConnection();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}