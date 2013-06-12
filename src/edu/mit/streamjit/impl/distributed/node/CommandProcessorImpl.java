/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.node;

import edu.mit.streamjit.impl.distributed.api.CommandProcessor;

public class CommandProcessorImpl implements CommandProcessor {
	StreamNode slave;

	public CommandProcessorImpl(StreamNode slave) {
		this.slave = slave;
	}

	@Override
	public void processSTART() {
		System.out.println("StraemJit app started...");
		slave.getBlobsManager().start();
	}

	@Override
	public void processSTOP() {
		System.out.println("StraemJit app stopped...");
		slave.getBlobsManager().stop();
	}

	@Override
	public void processSUSPEND() {
		slave.getBlobsManager().suspend();
	}

	@Override
	public void processRESUME() {
		slave.getBlobsManager().resume();
	}

	@Override
	public void processEXIT() {
		System.out.println("StreamNode is Exiting...");
		slave.exit();
	}
}
