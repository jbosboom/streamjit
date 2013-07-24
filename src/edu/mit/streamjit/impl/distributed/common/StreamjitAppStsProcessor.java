/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.common;


public class StreamjitAppStsProcessor implements AppStatusProcessor {

	@Override
	public void processRUNNING() {
		System.out.println("I am processing Running");
	}

	@Override
	public void processSTOPPED() {
		System.out.println("I am processing Stopped");
	}

	@Override
	public void processERROR() {
		System.out.println("I am processing Error");
	}

	@Override
	public void processNOT_STARTED() {
		System.out.println("I am processing Not Started");
	}

	@Override
	public void processWAITING() {
		System.out.println("I am processing Waiting");
	}

}
