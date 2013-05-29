/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.slave;

import edu.mit.streamjit.impl.distributed.runtime.api.ErrorProcessor;

public class SlaveErrorProcessor implements ErrorProcessor {

	@Override
	public void processFILE_NOT_FOUND() {
		throw new IllegalArgumentException("FILE_NOT_FOUND error should be informed to Master");
	}

	@Override
	public void processWORKER_NOT_FOUND() {
		throw new IllegalArgumentException("WORKER_NOT_FOUND error should be informed to Master");
	}
}