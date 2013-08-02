package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * Processes the {@link Command}s received from {@link Controller}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
public interface CommandProcessor {

	public void processSTART();

	public void processSTOP();

	public void processSUSPEND();

	public void processRESUME();

	public void processEXIT();
}
