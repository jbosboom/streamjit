package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.common.TimeLogger;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo.CompilationTime;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo.DrainingTime;
import edu.mit.streamjit.impl.distributed.common.SNTimeInfo.SNTimeInfoProcessor;

/**
 * Uses {@link TimeLogger} to log timing information.
 * 
 * @author sumanan
 * @since Nov 24, 2014
 */
public class SNTimeInfoProcessorImpl implements SNTimeInfoProcessor {

	private final TimeLogger logger;

	public SNTimeInfoProcessorImpl(TimeLogger logger) {
		this.logger = logger;
	}

	@Override
	public void process(CompilationTime compilationTime) {
		String msg = String.format("Blob-%s-%.0fms\n", compilationTime.blobID,
				compilationTime.milliSec);
		logger.logCompileTime(msg);
	}

	@Override
	public void process(DrainingTime drainingTime) {
		String msg = String.format("Blob-%s-%.0fms\n", drainingTime.blobID,
				drainingTime.milliSec);
		logger.logDrainTime(msg);
	}
}