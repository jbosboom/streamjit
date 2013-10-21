package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.distributed.common.SNException;
import edu.mit.streamjit.impl.distributed.common.SNException.AddressBindException;
import edu.mit.streamjit.impl.distributed.common.SNException.MakeBlobException;
import edu.mit.streamjit.impl.distributed.common.SNException.SNExceptionProcessor;

public class SNExceptionProcessorImpl implements SNExceptionProcessor {

	public SNExceptionProcessorImpl() {

	}

	@Override
	public void process(SNException ex) {
		// Print the exp msg.
	}

	@Override
	public void process(AddressBindException abEx) {
		// TODO Send new address
	}

	@Override
	public void process(MakeBlobException mbEx) {
		// TODO Stop the execution and go for a new configuration.
		
	}
}
