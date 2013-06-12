package edu.mit.streamjit.impl.distributed.api;

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

	void closeConnection() throws IOException;

	boolean isStillConnected();

	void makeConnection() throws IOException;

	Runnable getRunnable();

	int getOtherMachineID();

	/**
	 * Stop the actions. If it is {@link BoundaryOutputChannel} then stop sending, if {@link BoundaryInputChannel} then stop receiving.
	 */
	void stop();
}
