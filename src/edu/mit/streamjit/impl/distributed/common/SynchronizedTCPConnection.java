package edu.mit.streamjit.impl.distributed.common;

import java.io.IOException;
import java.net.Socket;

/**
 * This is a thread safe {@link TCPConnection}. Write and read operations are
 * thread safe and both are synchronized separately.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Oct 16, 2013
 */
public class SynchronizedTCPConnection implements Connection {

	private final TCPConnection tcpConnection;

	private final Object writeLock = new Object();
	private final Object readLock = new Object();

	public SynchronizedTCPConnection(Socket socket) {
		this.tcpConnection = new TCPConnection(socket);
	}

	@Override
	public <T> T readObject() throws IOException, ClassNotFoundException {
		synchronized (readLock) {
			return tcpConnection.readObject();
		}
	}

	@Override
	public void writeObject(Object obj) throws IOException {
		synchronized (writeLock) {
			tcpConnection.writeObject(obj);
		}
	}

	@Override
	public void closeConnection() throws IOException {
		tcpConnection.closeConnection();
	}

	@Override
	public void softClose() throws IOException {
		tcpConnection.softClose();
	}

	@Override
	public boolean isStillConnected() {
		return tcpConnection.isStillConnected();
	}
}
