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

	public static TCPConnection getConnection(String serverAddress, int portNo)
			throws IOException {
		Ipv4Validator validator = Ipv4Validator.getInstance();

		if (!validator.isValid(serverAddress))
			throw new IllegalArgumentException("Invalid Server IP address");

		if (!validator.isValid(portNo))
			throw new IllegalArgumentException("Invalid port No");

		int maxTryAttempts = 10;
		for (int i = 0; i < maxTryAttempts; i++) {
			try {
				Socket socket = new Socket(serverAddress, portNo);
				return new TCPConnection(socket);
			} catch (IOException ioe) {
				System.out.println("IO Connection failed");
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
	public static TCPConnection getConnection(int portNo, int timeOut)
			throws IOException {
		ListenerSocket listnerSckt = new ListenerSocket(portNo);
		Socket socket = listnerSckt.makeConnection(timeOut);
		return new TCPConnection(socket);
	}

	public static Connection getConnection(Socket socket) throws IOException {
		if (socket == null)
			throw new IOException("Null Socket.");
		return new TCPConnection(socket);
	}
}
