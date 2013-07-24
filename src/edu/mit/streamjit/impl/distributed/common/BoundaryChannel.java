package edu.mit.streamjit.impl.distributed.common;

import java.io.IOException;

import edu.mit.streamjit.impl.blob.Buffer;

/**
 * {@link BoundaryChannel} wraps a {@link Buffer} that crosses over the
 * machine(node) boundary. Potentially these buffers may depend on a I/O
 * communication method to send or receive data with the peer node. As
 * {@link BoundaryChannel}s are meant to run on a independent I/O thread, the
 * wrapped {@link Buffer} must be thread safe.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 28, 2013
 */
public interface BoundaryChannel {

	/**
	 * Close the connection.
	 * 
	 * @throws IOException
	 */
	void closeConnection() throws IOException;

	/**
	 * @return true iff the connection with the other node is still valid.
	 */
	boolean isStillConnected();

	/**
	 * @return {@link Runnable} that does all IO communication and send
	 *         data(stream tuples) to other node (or receive from other node).
	 */
	Runnable getRunnable();

	/**
	 * @return Other end of the node's ID.
	 */
	int getOtherNodeID();

	/**
	 * Stop the actions. If it is {@link BoundaryOutputChannel} then stop
	 * sending, if {@link BoundaryInputChannel} then stop receiving.
	 */
	void stop();

	/**
	 * Interface that represents input channels.
	 */
	public interface BoundaryInputChannel extends BoundaryChannel {

		/**
		 * Receive data from other node.
		 */
		void receiveData();
	}

	/**
	 * Interface that represents output channels.
	 */
	public interface BoundaryOutputChannel extends BoundaryChannel {

		/**
		 * Send data to other node.
		 */
		void sendData();
	}
}
