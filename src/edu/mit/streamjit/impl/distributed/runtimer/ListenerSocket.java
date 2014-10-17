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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ListenerSocket} listens for new TCP connections. This class can be
 * used in two different ways.
 * <ul>
 * <li>First, It can be made to run on separate thread and keep on listening
 * until the stop condition is fulfilled. In this separate thread way, again
 * ListenerSocket can be used in two different ways.
 * <ol>
 * <li>Caller can ask {@link ListenerSocket} to listen until it asks to stop.</li>
 * <li>Caller can mention expected number of sockets that should be accepted. In
 * this case, listener thread will stop listening once it has accepted the
 * expected count.</li>
 * </ol>
 * </li>
 * <li>Second, without running this on a separate thread, caller thread may call
 * {@link #makeConnection(int)} to establish connections.</li>
 * </ul>
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 14, 2013
 */
public final class ListenerSocket extends Thread {

	private ServerSocket server;

	private AtomicBoolean keepOnListen;

	private int expectedConnections;

	private ConcurrentLinkedQueue<Socket> acceptedSockets;

	/**
	 * @param portNo
	 *            Listening port number.
	 * @param expectedConnections
	 *            : ListenerSocket will try to accept at most this amount
	 *            sockets. Once this limit is reached, {@link ListenerSocket}
	 *            thread will die itself.
	 * @throws IOException
	 */
	public ListenerSocket(int portNo, int expectedConnections)
			throws IOException {
		super("Listener Socket");
		try {
			server = new ServerSocket(portNo);
			this.expectedConnections = expectedConnections;
		} catch (IOException e) {
			System.out.println("Could not listen on port " + portNo);
			throw e;
		}

		acceptedSockets = new ConcurrentLinkedQueue<>();
		keepOnListen = new AtomicBoolean(true);
	}

	/**
	 * {@link ListenerSocket} will accept as much sockets as it can until
	 * stopListening() is called.
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
				Socket socket = server.accept();
				acceptedSockets.add(socket);
				connectionCount++;
			} catch (IOException e) {
				// TODO What to do if IO exception occurred?
				// 1. Abort the listening?
				// 2. Close the socket and recreate new one?
				// 3. Wait for some time and then keep on listen on the same
				// socket.
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

	/**
	 * This function accept a socket and returns it.
	 * 
	 * @param timeOut
	 *            - socket time out. See {@link ServerSocket}.setSoTimeout().
	 * @return accepted {@link Socket}
	 * @throws SocketTimeoutException
	 * @throws IOException
	 */
	public Socket makeConnection(int timeOut) throws SocketTimeoutException,
			IOException {
		try {
			server.setSoTimeout(timeOut);
			Socket socket = server.accept();
			server.setSoTimeout(0);
			return socket;
		} catch (SocketTimeoutException stEx) {
			throw stEx;
		} catch (IOException e) {
			// TODO What to do if IO exception occurred?
			// 1. Abort the listening?
			// 2. Close the socket and recreate new one?
			// 3. Wait for some time and then keep on listen on the same socket.
			// Currently case 3 is done.
			e.printStackTrace();
			throw e;
		}
	}

	/**
	 * Stops listening for new connections and stop the thread as well.
	 */
	public void stopListening() {
		keepOnListen.set(false);
	}

	/**
	 * @return The sockets those are accepted since last function call. i.e,
	 *         does not return all accepted socket since started.
	 */
	public List<Socket> getAcceptedSockets() {
		List<Socket> acceptedSocketslist = new LinkedList<>();
		while (!acceptedSockets.isEmpty()) {
			acceptedSocketslist.add(acceptedSockets.poll());
		}
		return acceptedSocketslist;
	}
}