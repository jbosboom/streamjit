package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;

/**
 * {@link CommandProcessor} at {@link Controller} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Aug 11, 2013
 */
public class CNCommandProcessorImpl implements CommandProcessor {

	@Override
	public void processSTART() {
		throw new IllegalArgumentException(
				"Command shouldn't be received by controller.");
	}

	@Override
	public void processSTOP() {
		throw new IllegalArgumentException(
				"Command shouldn't be received by controller.");
	}

	@Override
	public void processDRAIN() {
		throw new IllegalArgumentException(
				"Command shouldn't be received by controller.");
	}

	@Override
	public void processEXIT() {
		throw new IllegalArgumentException(
				"Command shouldn't be received by controller.");
	}

}
