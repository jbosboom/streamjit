package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainProcessor;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Aug 13, 2013
 */
public class DistributedDrainer extends AbstractDrainer {

	Controller controller;

	public DistributedDrainer(Controller controller) {
		this.controller = controller;
		SNDrainProcessor dp = new CNDrainProcessorImpl(this);
		controller.setDrainProcessor(dp);
	}

	@Override
	protected void drainingDone(boolean isFinal) {
		controller.drainingFinished(isFinal);
	}

	@Override
	protected void drain(Token blobID, boolean isFinal) {
		controller.drain(blobID, isFinal);
	}

	@Override
	protected void drainingDone(Token blobID, boolean isFinal) {
		// Nothing to clean in Distributed case.
	}

	@Override
	protected void prepareDraining(boolean isFinal) {
		controller.drainingStarted(isFinal);
	}
}
