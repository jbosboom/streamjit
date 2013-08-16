package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationStringProcessor;

/**
 * {@link ConfigurationStringProcessor} at {@link Controller} side.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Aug 11, 2013
 */
public class CNCfgStringProcessorImpl implements ConfigurationStringProcessor {

	@Override
	public void process(String cfg) {
		throw new IllegalArgumentException(
				"Configuraion string shouldn't be received by controller.");
	}
}
