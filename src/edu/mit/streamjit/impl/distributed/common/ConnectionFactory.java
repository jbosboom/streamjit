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
package edu.mit.streamjit.impl.distributed.common;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import edu.mit.streamjit.impl.distributed.runtimer.ListenerSocket;

/**
 * Returns {@link Connection}s. Ask this {@link ConnectionFactory} for a new
 * connection.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jun 12, 2013
 */
public class ConnectionFactory {

	public static TCPConnection getConnection(String serverAddress, int portNo,
			boolean needSync) throws IOException {
		Ipv4Validator validator = Ipv4Validator.getInstance();
		System.out.println("Trying to make a connection with - "
				+ serverAddress + "/" + portNo);
		if (!validator.isValid(serverAddress))
			throw new IllegalArgumentException("Invalid Server IP address");

		if (!validator.isValid(portNo))
			throw new IllegalArgumentException("Invalid port No");

		int maxTryAttempts = 10;
		for (int i = 0; i < maxTryAttempts; i++) {
			try {
				Socket socket = new Socket(serverAddress, portNo);
				if (needSync)
					return new SynchronizedTCPConnection(socket);
				else
					return new TCPConnection(socket);
			} catch (IOException ioe) {
				System.out.println("IO Connection failed - " + serverAddress
						+ "/" + portNo);
				if (i == maxTryAttempts - 1)
					throw ioe;
				System.out.println("Reattempting...." + i);
			}
			try {
				Thread.sleep((i + 1) * 2000); // increase the sleep time by 2S
												// for every iteration.
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		throw new IOException("Connection creation failed.");
	}

	/**
	 * @param portNo
	 * @param timeOut
	 *            in milliseconds. If zero, no timeout. See {@link ServerSocket}
	 *            .setSoTimeout().
	 * @return
	 * @throws IOException
	 */
	public static TCPConnection getConnection(int portNo, int timeOut,
			boolean needSync) throws IOException {
		System.out.println("Listening at - " + portNo);
		ListenerSocket listnerSckt = new ListenerSocket(portNo);
		Socket socket = listnerSckt.makeConnection(timeOut);
		if (needSync)
			return new SynchronizedTCPConnection(socket);
		else
			return new TCPConnection(socket);
	}

	public static Connection getConnection(Socket socket, boolean needSync)
			throws IOException {
		if (socket == null)
			throw new IOException("Null Socket.");
		if (needSync)
			return new SynchronizedTCPConnection(socket);
		else
			return new TCPConnection(socket);
	}
}
