package edu.mit.streamjit.impl.distributed.node;

import java.io.IOException;

/**
 * StreamNode side communication interface.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 14, 2013
 */
public interface Connection {

	public <T> T readObject() throws IOException, ClassNotFoundException;

	public boolean writeObject(Object obj) throws IOException;

	public boolean closeConnection() throws IOException;

	public boolean isStillConnected();
}
