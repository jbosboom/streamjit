package edu.mit.streamjit.api;

import edu.mit.streamjit.api.Filter;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public abstract class StatefulFilter<I, O> extends Filter<I, O> {
	public StatefulFilter(int popRate, int pushRate, int peekRate) {
		super(popRate, pushRate, peekRate);
	}
}
