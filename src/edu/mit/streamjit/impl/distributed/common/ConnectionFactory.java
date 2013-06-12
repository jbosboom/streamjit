package edu.mit.streamjit.impl.distributed.common;

import java.io.IOException;
import java.net.Socket;

import edu.mit.streamjit.impl.distributed.node.Connection;

/**
 * Returns {@link Connection}s
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jun 12, 2013
 */
public class ConnectionFactory {
	
	public Connection getConnection(Socket socket) {
		return new TCPConnection(socket);
	}

	public Connection getConnection(String serverAddress, int portNo) throws IOException {
		Ipv4Validator validator = Ipv4Validator.getInstance();

		if (!validator.isValid(serverAddress))
			throw new IllegalArgumentException("Invalid Server IP address");

		if (!validator.isValid(portNo))
			throw new IllegalArgumentException("Invalid port No");

		int maxTryAttempts = 5;
		for (int i = 0; i < maxTryAttempts; i++) {
			try {
				Socket socket = new Socket(serverAddress, portNo);
				System.out.println("Connection with controller established");
				return new TCPConnection(socket);
			} catch (IOException ioe) {
				System.out.println("IO Connection failed");
				if (i == maxTryAttempts - 1)
					throw ioe;
				System.out.println("Reattempting...." + i);
			}

			try {
				Thread.sleep((i + 1) * 2000); // increase the sleep time by 2S for every iteration.
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		throw new IOException("Connection creation failed.");
	}
}
