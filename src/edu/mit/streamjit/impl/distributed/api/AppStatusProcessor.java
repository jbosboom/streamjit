/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.api;

public interface AppStatusProcessor {

	public void processRUNNING();

	public void processSTOPPED();

	public void processERROR();

	public void processNOT_STARTED();

	public void processWAITING();
}
