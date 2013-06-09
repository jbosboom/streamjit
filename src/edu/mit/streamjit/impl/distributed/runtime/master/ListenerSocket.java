package edu.mit.streamjit.impl.distributed.runtime.master;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.mit.streamjit.impl.distributed.runtime.common.TCPSocket;

/**
 * {@link ListenerSocket} listens for new TCP connections. It can run on separate thread and keep on listening until the stop condition
 * is full filled. {@link ListenerSocket} can be used in two different ways.
 * 1) Caller can ask {@link ListenerSocket} to listen until it asks to stop 2) Caller can mention expected number of sockets that
 * should be accepted. In this case, listener thread will stop listening once it has accepted the expected count.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 14, 2013
 */
public class ListenerSocket extends Thread {

	private ServerSocket server;

	private AtomicBoolean keepOnListen;

	private int expectedConnections;

	private ConcurrentLinkedQueue<TCPSocket> acceptedSockets;

	public void stopListening() {
		keepOnListen.set(false);
	}

	// Only returns the sockets those are accepted since this last function call.
	public List<TCPSocket> getAcceptedSockets() {
		List<TCPSocket> acceptedSocketslist = new LinkedList<>();
		while (!acceptedSockets.isEmpty()) {
			acceptedSocketslist.add(acceptedSockets.poll()); // removes from the acceptedSockets and add it to the acceptedSocketslist.
		}
		return acceptedSocketslist;
	}

	/**
	 * @param portNo
	 * @param expectedConnections
	 *            : ListenerSocket will try to accept at most this no of sockets. Once this limit is reached, {@link ListenerSocket}
	 *            thread will die it self.
	 * @throws IOException
	 */
	public ListenerSocket(int portNo, int expectedConnections) throws IOException {
		try {
			server = new ServerSocket(portNo);
			this.expectedConnections = expectedConnections;
		} catch (IOException e) {
			System.out.println("Could not listen on port " + portNo);
			throw e;
		}

		acceptedSockets = new ConcurrentLinkedQueue<>();
		this.keepOnListen = new AtomicBoolean();
		keepOnListen.set(true);
	}

	/**
	 * {@link ListenerSocket} will accept as much sockets as it can until stopListening() is called.
	 * 
	 * @param portNo
	 * @throws IOException
	 */
	public ListenerSocket(int portNo) throws IOException {
		this(portNo, Integer.MAX_VALUE);
	}

	public void run() {
		int connectionCount = 0;
		while (keepOnListen.get() && connectionCount < this.expectedConnections) {
			try {
				TCPSocket slvSckt = acceptConnection();
				acceptedSockets.add(slvSckt);
				connectionCount++;
			} catch (IOException e) {
				// TODO What to do if IO exception occurred?
				// 1. Abort the listening?
				// 2. Close the socket and recreate new one?
				// 3. Wait for some time and then keep on listen on the same socket.
				// Currently case 3 is done.
				e.printStackTrace();
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}

	private TCPSocket acceptConnection() throws IOException {
		Socket socket;
		try {
			socket = server.accept();
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		TCPSocket slvSckt = new TCPSocket(socket);
		return slvSckt;
	}
}