/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.node;

import edu.mit.streamjit.impl.distributed.api.CommandProcessor;

public class CommandProcessorImpl implements CommandProcessor {
	StreamNode streamNode;

	public CommandProcessorImpl(StreamNode streamNode) {
		this.streamNode = streamNode;
	}

	@Override
	public void processSTART() {
		System.out.println("StraemJit app started...");
		streamNode.getBlobsManager().start();
	}

	@Override
	public void processSTOP() {
		System.out.println("StraemJit app stopped...");
		streamNode.getBlobsManager().stop();
	}

	@Override
	public void processSUSPEND() {
		streamNode.getBlobsManager().suspend();
	}

	@Override
	public void processRESUME() {
		streamNode.getBlobsManager().resume();
	}

	@Override
	public void processEXIT() {
		System.out.println("StreamNode is Exiting...");
		streamNode.exit();
	}
}