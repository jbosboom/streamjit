package edu.mit.streamjit.impl.distributed.common;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

	public static AsynchronousTCPConnection getAsyncConnection(int portNo)
			throws IOException {
		AsynchronousServerSocketChannel ssc;
		AsynchronousSocketChannel sc2;
		System.out.println("Inside initialization");
		InetSocketAddress isa = new InetSocketAddress("", portNo);

		ssc = AsynchronousServerSocketChannel.open().bind(isa);
		Future<AsynchronousSocketChannel> accepted = ssc.accept();
		try {
			sc2 = accepted.get();
		} catch (InterruptedException | ExecutionException ex) {
			ex.printStackTrace();
			return null;
		}

		ssc.close();
		return new AsynchronousTCPConnection(sc2);
	}
}
