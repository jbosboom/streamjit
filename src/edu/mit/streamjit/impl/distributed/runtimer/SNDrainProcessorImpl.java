package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.Drained;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.DrainedData;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainProcessor;

/**
 * {@link DrainProcessor} at {@link Controller} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Aug 11, 2013
 */
public class SNDrainProcessorImpl implements SNDrainProcessor {

	AbstractDrainer drainer;

	public SNDrainProcessorImpl(AbstractDrainer drainer) {
		this.drainer = drainer;
	}

	@Override
	public void process(Drained drained) {
		drainer.drained(drained.blobID);
	}

	@Override
	public void process(DrainedData drainedData) {
		drainer.newDrainData(drainedData);
	}
}
