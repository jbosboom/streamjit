package org.mit.jstreamit;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public abstract class Filter<I, O> implements StreamElement<I, O> {
	public abstract void work();

	//These don't actually do anything, they're just for the JIT to pattern-match against.
	protected final I pop() {return null;};
	protected final I peek(int position) {return null;};
	protected final void push(O item) {};
}
