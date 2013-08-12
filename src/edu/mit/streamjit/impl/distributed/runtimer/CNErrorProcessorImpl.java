package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.distributed.common.Error;
import edu.mit.streamjit.impl.distributed.common.Error.ErrorProcessor;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.StreamNodeAgent;

/**
 * {@link ErrorProcessor} at {@link Controller} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Aug 11, 2013
 */
public class CNErrorProcessorImpl implements ErrorProcessor {

	StreamNodeAgent streamNode;

	public CNErrorProcessorImpl(StreamNodeAgent streamNode) {
		this.streamNode = streamNode;
	}

	@Override
	public void processFILE_NOT_FOUND() {
		streamNode.setError(Error.FILE_NOT_FOUND);
	}

	@Override
	public void processWORKER_NOT_FOUND() {
		streamNode.setError(Error.WORKER_NOT_FOUND);
	}

	@Override
	public void processBLOB_NOT_FOUND() {
		streamNode.setError(Error.BLOB_NOT_FOUND);
	}
}
