/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.slave;

import edu.mit.streamjit.impl.distributed.runtime.api.Request;
import edu.mit.streamjit.impl.distributed.runtime.api.RequestProcessor;

public class SlaveRequestProcessor implements RequestProcessor{
	
	SlaveConnection masterConnection;
	
	SlaveRequestProcessor(SlaveConnection masterConnection)
	{
		this.masterConnection = masterConnection;
	}

	@Override
	public void processAPPStatus() {
		
	}

	@Override
	public void processSysInfo() {
		
	}

	@Override
	public void processMaxCores() {
		
	}

}
