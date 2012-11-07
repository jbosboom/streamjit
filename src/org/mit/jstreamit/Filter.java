package org.mit.jstreamit;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public abstract class Filter<I, O> implements StreamElement<I, O> {
	//TODO: eventually we'll want to allow ranges, typical values and possibly
	//sets of rates (for when the work-function has either rate A or rate B
	//depending on state).
	private final int popRate, pushRate, peekRate;
	public Filter(int popRate, int pushRate, int peekRate) {
		this.popRate = popRate;
		this.pushRate = pushRate;
		this.peekRate = peekRate;
	}
	public abstract void work();

	//These don't actually do anything, they're just for the JIT to pattern-match against.
	protected final I pop() {return null;};
	protected final I peek(int position) {return null;};
	protected final void push(O item) {};
}
