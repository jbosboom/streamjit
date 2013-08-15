package edu.mit.streamjit.impl.distributed.runtimer;

import static com.google.common.base.Preconditions.*;

import edu.mit.streamjit.impl.common.BlobGraph;
import edu.mit.streamjit.impl.common.BlobGraph.BlobNode;
import edu.mit.streamjit.impl.distributed.common.DrainElement.DoDrain;
import edu.mit.streamjit.impl.distributed.common.DrainElement.DrainDataRequest;
import edu.mit.streamjit.impl.distributed.common.DrainElement.DrainProcessor;
import edu.mit.streamjit.impl.distributed.common.DrainElement.Drained;
import edu.mit.streamjit.impl.distributed.common.DrainElement.DrainedDataMap;

/**
 * {@link DrainProcessor} at {@link Controller} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Aug 11, 2013
 */
public class CNDrainProcessorImpl implements DrainProcessor {

	BlobGraph blobGraph;

	public CNDrainProcessorImpl(BlobGraph blobGraph) {
		this.blobGraph = blobGraph;
	}

	@Override
	public void process(DrainDataRequest drnDataReq) {
		throw new IllegalArgumentException(
				"DrainDataRequest shouldn't be received by controller.");
	}

	@Override
	public void process(DoDrain drain) {
		throw new IllegalArgumentException(
				"DoDrain shouldn't be received by controller.");
	}

	@Override
	public void process(Drained drained) {
		BlobNode bn = blobGraph.getBlobNode(drained.blobID);
		checkNotNull(bn);
		bn.drained();
	}

	@Override
	public void process(DrainedDataMap drainedData) {

	}
}
