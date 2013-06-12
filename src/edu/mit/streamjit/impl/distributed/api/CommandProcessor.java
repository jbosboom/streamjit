/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.api;

public interface CommandProcessor {

	public void processSTART();

	public void processSTOP();
	
	public void processSUSPEND();
	
	public void processRESUME();
	
	public void processEXIT();
}
