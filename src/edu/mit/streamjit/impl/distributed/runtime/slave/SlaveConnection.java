package edu.mit.streamjit.impl.distributed.runtime.slave;

import java.io.IOException;

/**
 * Slave side communication interface.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 14, 2013
 */
public interface SlaveConnection {

	public <T> T readObject(Class<T> Klass) throws IOException, ClassNotFoundException;

	public boolean writeObject(Object obj) throws IOException;

	public boolean closeConnection() throws IOException;

	public boolean isStillConnected();

	public boolean makeConnection() throws IOException;
}
