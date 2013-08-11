package edu.mit.streamjit.impl.distributed.node;

import edu.mit.streamjit.impl.distributed.common.DrainElement.DoDrain;
import edu.mit.streamjit.impl.distributed.common.DrainElement.DrainDataRequest;
import edu.mit.streamjit.impl.distributed.common.DrainElement.DrainProcessor;
import edu.mit.streamjit.impl.distributed.common.DrainElement.Drained;
import edu.mit.streamjit.impl.distributed.common.DrainElement.DrainedDataMap;

/**
 * Implementation of {@link DrainProcessor} at {@link StreamNode} side. All
 * appropriate response logic to successfully perform the draining is
 * implemented here.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jul 30, 2013
 */
public class SNDrainProcessorImpl implements DrainProcessor {

	StreamNode streamNode;

	public SNDrainProcessorImpl(StreamNode streamNode) {
		this.streamNode = streamNode;
	}

	@Override
	public void process(DrainDataRequest drnDataReq) {
		streamNode.getBlobsManager().reqDrainedData(drnDataReq.blobsSet);
	}

	@Override
	public void process(DoDrain drain) {
		streamNode.getBlobsManager().drain(drain.blobID, drain.reqDrainData);
	}

	@Override
	public void process(Drained drained) {
		throw new IllegalArgumentException(
				"Stream Node shall not receive this object from controller.");
	}

	@Override
	public void process(DrainedDataMap drainedData) {
		throw new IllegalArgumentException(
				"Stream Node shall not receive this object from controller.");
	}

}
