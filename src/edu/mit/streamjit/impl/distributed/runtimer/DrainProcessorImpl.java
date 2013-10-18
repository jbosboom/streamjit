package edu.mit.streamjit.impl.distributed.runtimer;

import java.util.Map;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.Drained;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.DrainedDataMap;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainProcessor;

/**
 * {@link DrainProcessor} at {@link Controller} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Aug 11, 2013
 */
public class DrainProcessorImpl implements SNDrainProcessor {

	AbstractDrainer drainer;

	public DrainProcessorImpl(AbstractDrainer drainer) {
		this.drainer = drainer;
	}

	@Override
	public void process(Drained drained) {
		drainer.drained(drained.blobID);
	}

	@Override
	public void process(DrainedDataMap drainedData) {
		for (Map.Entry<Token, DrainData> entry : drainedData.drainData
				.entrySet()) {
			drainer.newDrainData(entry.getKey(), entry.getValue());
		}
	}
}
