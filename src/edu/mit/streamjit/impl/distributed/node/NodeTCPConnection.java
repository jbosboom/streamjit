/**
 * @author Sumanan sumanan@mit.edu
 * @since May 14, 2013
 */
package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import edu.mit.streamjit.impl.distributed.common.Ipv4Validator;
import edu.mit.streamjit.impl.distributed.common.TCPSocket;

public class NodeTCPConnection implements NodeConnection {

	private String serverAddress;
	private int portNo;

	private TCPSocket socket;

	NodeTCPConnection(String serverAddress, int portNo) {
		Ipv4Validator validator = Ipv4Validator.getInstance();

		if (validator.isValid(serverAddress))
			this.serverAddress = serverAddress;
		else
			throw new IllegalArgumentException("Invalid Server IP address");

		if (validator.isValid(portNo))
			this.portNo = portNo;
		else
			throw new IllegalArgumentException("Invalid port No");
	}

	@Override
	public <T> T readObject() throws IOException, ClassNotFoundException {
		try {
			return socket.receiveObject();
		} catch (IOException | ClassNotFoundException e) {
			throw e;
		}
	}

	@Override
	public boolean writeObject(Object obj) throws IOException {

		socket.sendObject(obj);
		return true;
	}

	@Override
	public boolean closeConnection() throws IOException {
		if (socket != null)
			socket.closeConnection();
		return true;
	}

	@Override
	public boolean isStillConnected() {
		return (socket == null) ? false : socket.isStillconnected();
	}

	@Override
	public boolean makeConnection() throws IOException {
		// Try 5 times...
		int maxTryAttempts = 5;
		for (int i = 0; i < maxTryAttempts; i++) {
			try {
				Socket socket = new Socket(this.serverAddress, this.portNo);
				this.socket = new TCPSocket(socket);
				System.out.println("Connection with master established");
				return true;
			} catch (UnknownHostException uhe) {
				System.out.println("Unknown Server Address");
				uhe.printStackTrace();
				return false;
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
		return false;
	}
}
