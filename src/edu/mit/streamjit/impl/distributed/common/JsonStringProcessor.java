package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * Processes json string of a {@link Configuration} that is sent by
 * {@link Controller}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public interface JsonStringProcessor {

	public void process(String json);

}
