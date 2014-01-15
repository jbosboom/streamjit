package edu.mit.streamjit.impl.distributed.node;

import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.CTRLRDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;

/**
 * {@link BlobsManager} is the main dispatcher for all blobs. Received commands
 * will call the appropriate BlobsManager's functions and the BlobsManager will
 * take responsibility to execute the command on all assigned {@link Blob}s.
 * </p> TODO: Draining mechanism need to be added.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 15, 2013
 */
public interface BlobsManager {

	public CTRLRDrainProcessor getDrainProcessor();

	public CommandProcessor getCommandProcessor();
}
