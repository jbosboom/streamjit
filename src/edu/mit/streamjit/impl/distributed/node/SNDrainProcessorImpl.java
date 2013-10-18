package edu.mit.streamjit.impl.distributed.node;

import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.CTRLRDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DoDrain;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.DrainDataRequest;

/**
 * Implementation of {@link DrainProcessor} at {@link StreamNode} side. All
 * appropriate response logic to successfully perform the draining is
 * implemented here.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jul 30, 2013
 */
public class SNDrainProcessorImpl implements CTRLRDrainProcessor {

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
}
