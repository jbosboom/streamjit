package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.AppStatus.AppStatusProcessor;
import edu.mit.streamjit.impl.distributed.runtimer.CommunicationManager.StreamNodeAgent;

/**
 * {@link AppStatusProcessor} at {@link Controller} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Aug 11, 2013
 */
public class CNAppStatusProcessorImpl implements AppStatusProcessor {

	StreamNodeAgent streamNode;

	public CNAppStatusProcessorImpl(StreamNodeAgent streamNode) {
		this.streamNode = streamNode;
	}

	@Override
	public void processRUNNING() {
		streamNode.setAppStatus(AppStatus.RUNNING);
	}

	@Override
	public void processSTOPPED() {
		streamNode.setAppStatus(AppStatus.STOPPED);
	}

	@Override
	public void processERROR() {
		streamNode.setAppStatus(AppStatus.ERROR);
	}

	@Override
	public void processNOT_STARTED() {
		streamNode.setAppStatus(AppStatus.NOT_STARTED);
	}

	@Override
	public void processNO_APP() {
		streamNode.setAppStatus(AppStatus.NO_APP);
	}
}
