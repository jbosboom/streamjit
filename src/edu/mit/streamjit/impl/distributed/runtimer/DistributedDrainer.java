package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.common.BlobGraph;
import edu.mit.streamjit.impl.common.BlobGraph.AbstractDrainer;
import edu.mit.streamjit.impl.common.BlobGraph.BlobNode;
import edu.mit.streamjit.impl.distributed.common.DrainElement.DrainProcessor;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Aug 13, 2013
 */
public class DistributedDrainer extends AbstractDrainer {

	Controller controller;

	public DistributedDrainer(BlobGraph blobGraph, boolean needDrainData,
			Controller controller) {
		super(blobGraph, needDrainData);
		this.controller = controller;
		DrainProcessor dp = new CNDrainProcessorImpl(blobGraph);
		controller.setDrainProcessor(dp);

	}

	@Override
	public void drain(BlobNode node) {
		controller.drain(node.getBlobID());
	}

	@Override
	public void drained(BlobNode node) {
	}

	@Override
	protected void drainingFinished() {
		controller.drainingFinished();
	}
}
