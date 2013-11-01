package edu.mit.streamjit.impl.distributed.node;

import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.MiscCtrlElementProcessor;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.NewConInfo;

public class MiscCtrlElementProcessorImpl implements MiscCtrlElementProcessor {

	private final StreamNode streamNode;

	MiscCtrlElementProcessorImpl(StreamNode streamNode) {
		this.streamNode = streamNode;
	}

	@Override
	public void process(NewConInfo newConInfo) {
		// TODO
		System.err.println("Need to process this soon");
	}
}
