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
public class SynchronizedTCPConnection extends TCPConnection {

	private final Object writeLock = new Object();
	private final Object readLock = new Object();

	public SynchronizedTCPConnection(Socket socket) {
		super(socket);
	}

	@Override
	public <T> T readObject() throws IOException, ClassNotFoundException {
		synchronized (readLock) {
			return super.readObject();
		}
	}

	@Override
	public void writeObject(Object obj) throws IOException {
		synchronized (writeLock) {
			super.writeObject(obj);
		}
	}
}
