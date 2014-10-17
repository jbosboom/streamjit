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
import java.net.Socket;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import edu.mit.streamjit.impl.distributed.common.Connection;
import edu.mit.streamjit.impl.distributed.common.ConnectionFactory;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.Request;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement;
import edu.mit.streamjit.impl.distributed.common.SynchronizedTCPConnection;
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

	private InetAddress inetAddress;

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
		List<Socket> acceptedSocketList;
		while (true) {
			acceptedSocketList = listnerSckt.getAcceptedSockets();
			for (Socket s : acceptedSocketList) {

				Connection connection = new SynchronizedTCPConnection(s);
				StreamNodeAgent snAgent = new StreamNodeAgentImpl(nodeID++,
						connection, s.getInetAddress());

				if (!s.getLocalAddress().isLoopbackAddress()
						&& inetAddress == null)
					inetAddress = s.getLocalAddress();

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
		if (inetAddress == null) {
			inetAddress = acceptedSocketList.get(0).getLocalAddress();
		}
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
		private final Connection connection;

		private final InetAddress address;

		private StreamNodeAgentImpl(int machineID, Connection connection,
				InetAddress address) {
			super(machineID);
			this.connection = connection;
			this.address = address;
		}

		@Override
		public void writeObject(Object obj) throws IOException {
			connection.writeObject(obj);
		}

		@Override
		public boolean isConnected() {
			return connection.isStillConnected();
		}

		@Override
		public InetAddress getAddress() {
			return address;
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
					SNMessageElement me = connection.readObject();
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
				connection.writeObject(Request.EXIT);
				connection.closeConnection();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public InetAddress getLocalAddress() {
		return inetAddress;
	}
}