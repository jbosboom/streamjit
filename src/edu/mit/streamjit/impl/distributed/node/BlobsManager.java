package edu.mit.streamjit.impl.distributed.node;

/**
 * BlobsManager is the main dispatcher for all blobs. Received commands will
 * call the appropriate {@link BlobsManager}'s functions and the functions will
 * take responsibility execute the command on all assigned {@link Blob}s through
 * {@link BlobExecuter}
 * 
 * TODO: Draining mechanism need to be added.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 15, 2013
 */
public interface BlobsManager {

	/**
	 * Start and execute the blobs. This function should be responsible to
	 * manage all CPU and I/O threads those are related to the {@link Blob}s.
	 */
	public void start();

	/**
	 * Stop all {@link Blob}s if running. No effect if a {@link Blob} is already
	 * stopped.
	 */
	public void stop();
}
