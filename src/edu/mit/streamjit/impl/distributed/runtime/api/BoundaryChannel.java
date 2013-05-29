package edu.mit.streamjit.impl.distributed.runtime.api;

import java.io.IOException;

import edu.mit.streamjit.impl.interp.Channel;

/**
 * {@link BoundaryChannel} wraps a {@link Channel} that crosses over the machine(node) boundary. Potentially these channels may depend
 * on a I/O communication method to send or receive data with the peer node. As {@link BoundaryChannel}s are meant to run on a
 * independent I/O thread, the passed {@link Channel} must be thread safe.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 28, 2013
 */
public interface BoundaryChannel<E> {

	public void closeConnection() throws IOException;

	public boolean isStillConnected();

	public void makeConnection() throws IOException;

	public Runnable getRunnable();
	
	public int getOtherMachineID();
}
