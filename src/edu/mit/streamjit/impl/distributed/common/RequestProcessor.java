/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.common;

public interface RequestProcessor {

	public void processAPPStatus();

	public void processSysInfo();

	public void processMaxCores();

	public void processMachineID();
	
	public void processNodeInfo();
}
