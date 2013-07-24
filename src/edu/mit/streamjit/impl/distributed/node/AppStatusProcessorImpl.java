/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.node;

import edu.mit.streamjit.impl.distributed.common.AppStatusProcessor;

public class AppStatusProcessorImpl implements AppStatusProcessor {

	@Override
	public void processRUNNING() {
		throw new IllegalArgumentException("App status shouldn't be received by a StreamNode");
	}

	@Override
	public void processSTOPPED() {		
		throw new IllegalArgumentException("App status shouldn't be received by a StreamNode");
	}

	@Override
	public void processERROR() {
		throw new IllegalArgumentException("App status shouldn't be received by a StreamNode");
	}

	@Override
	public void processNOT_STARTED() {
		throw new IllegalArgumentException("App status shouldn't be received by a StreamNode");
	}

	@Override
	public void processWAITING() {
		throw new IllegalArgumentException("App status shouldn't be received by a StreamNode");
	}

}
