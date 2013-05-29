/**
 * @author Sumanan sumanan@mit.edu
 * @since May 14, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.master;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.mit.streamjit.impl.distributed.runtime.common.TCPSocket;

public class ListenerSocket extends Thread {

	private ServerSocket server;

	private AtomicBoolean keepOnListen;

	private ConcurrentLinkedQueue<TCPSocket> acceptedSockets;

	public void stopListening() {
		keepOnListen.set(false);
	}

	// Only returns the sockets those are accepted since this last function call.
	public List<TCPSocket> getAcceptedSockets() {
		List<TCPSocket> acceptedSocketslist = new LinkedList<>();
		while (!acceptedSockets.isEmpty()) {
			acceptedSocketslist.add(acceptedSockets.poll());		// removes from the acceptedSockets and add it to the acceptedSocketslist.
		}
		return acceptedSocketslist;
	}

	public ListenerSocket(int portNo) throws IOException {
		try {
			server = new ServerSocket(portNo);
		} catch (IOException e) {
			System.out.println("Could not listen on port " + portNo);
			throw e;
		}

		acceptedSockets = new ConcurrentLinkedQueue<>();
		keepOnListen.set(true);
	}

	public void run() {
		while (keepOnListen.get()) {
			try {
				TCPSocket slvSckt = acceptConnection();
				acceptedSockets.add(slvSckt);
			} catch (IOException e) {
				// TODO What to do if IO exception occurred?
				// 1. Abort the listening?
				// 2. Close the socket and recreate new one?
				// 3. Wait for some time and then keep on listen on the same socket.
				// Currently does 3.
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