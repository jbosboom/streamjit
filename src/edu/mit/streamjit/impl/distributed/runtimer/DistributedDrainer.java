package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainType;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Aug 13, 2013
 */
public class DistributedDrainer extends AbstractDrainer {

	StreamJitAppManager manager;

	public DistributedDrainer(StreamJitAppManager manager) {
		this.manager = manager;
		// Read this. Don't let the "this" reference escape during construction
		// http://www.ibm.com/developerworks/java/library/j-jtp0618/
		manager.setDrainer(this);
	}

	@Override
	protected void drainingDone(boolean isFinal) {
		manager.drainingFinished(isFinal);
	}

	@Override
	protected void drain(Token blobID, DrainType drainType) {
		manager.drain(blobID, drainType);
	}

	@Override
	protected void drainingDone(Token blobID, boolean isFinal) {
		// Nothing to clean in Distributed case.
	}

	@Override
	protected void prepareDraining(boolean isFinal) {
		manager.drainingStarted(isFinal);
	}
}
