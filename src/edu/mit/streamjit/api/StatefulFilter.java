package edu.mit.streamjit.api;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/7/2012
 */
public abstract class StatefulFilter<I, O> extends Filter<I, O> {
	public StatefulFilter(int popRate, int pushRate, int peekRate) {
		super(popRate, pushRate, peekRate);
	}
	
	public StatefulFilter(int popRate, int pushRate) {
		super(popRate, pushRate);
	}
}
