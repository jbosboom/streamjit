package edu.mit.streamjit.impl.distributed.api;

import edu.mit.streamjit.impl.blob.Blob;

/**
 * This interface is to manage the execution of a single {@link Blob}. {@link BlobsManager} shall issue all commands.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 14, 2013
 */
public interface BlobExecuter {

	/**
	 * Start and execute the passed {@link Blob}. This function should be responsible to manage all CPU and I/O threads those are related to the {@link Blob}.
	 */
	public void start();
	
	/**
	 *  Stop the {@link Blob} if it is running. No effect if the {@link Blob} is already stopped.
	 */
	public void stop();
	
	/**
	 * suspends the running {@link Blob}. All states should be maintained so that {@link Blob}s can be resumed with out any correctness violations. 
	 */
	public void suspend();
	
	/**
	 * Resumes the suspended {@link Blob}. No effect, if the blob is in other status.
	 */
	public void resume();
	
}
