package org.mit.jstreamit;

/**
 * see comments on Splitter
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public abstract class Joiner<I, O> {
	/**
	 * Returns the number of input channels this Joiner instance may read
	 * from, or Integer.MAX_VALUE if there is no limit.
	 * TODO: maybe this should be part of the rate handling?
	 * @return
	 */
	public abstract int inputs();

	//These don't actually do anything, they're just for the JIT to pattern-match against.
	protected final I pop(int channel) {return null;};
	protected final I peek(int channel, int position) {return null;};
	protected final void push(O item) {};
}
