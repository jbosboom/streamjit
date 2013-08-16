package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.distributed.common.Request.RequestProcessor;

/**
 * {@link RequestProcessor} at {@link Controller} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Aug 11, 2013
 */
public class CNRequestProcessorImpl implements RequestProcessor {

	@Override
	public void processAPPStatus() {
		throw new IllegalArgumentException(
				"Request shouldn't be received by controller.");
	}

	@Override
	public void processSysInfo() {
		throw new IllegalArgumentException(
				"Request shouldn't be received by controller.");
	}

	@Override
	public void processMachineID() {
		throw new IllegalArgumentException(
				"Request shouldn't be received by controller.");
	}

	@Override
	public void processNodeInfo() {
		throw new IllegalArgumentException(
				"Request shouldn't be received by controller.");
	}
}
