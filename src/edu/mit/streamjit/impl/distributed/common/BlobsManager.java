/**
 * @author Sumanan sumanan@mit.edu
 * @since May 15, 2013
 */
package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.blob.Blob;

/**
 * BlobsManager is the main dispatcher for all blobs. Received commands will call the appropriate {@link BlobsManager}'s functions and
 * the functions will take responsibility execute the command on all assigned {@link Blob}s through {@link BlobExecuter}
 */
public interface BlobsManager {

	public void start();

	public void stop();

	public void suspend();

	public void resume();
}
