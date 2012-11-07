package org.mit.jstreamit;

/**
 * TODO: splitters with multiple output types?  would sacrifice CTTS.
 * Splitters are not StreamElements because they have multiple outputs and thus
 * can't be used everywhere a StreamElement can.
 * Splitter is an abstract class rather than an interface to allow the library
 * to handle rate info management.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public abstract class Splitter<I, O> {
	public static final int UNLIMITED = Integer.MAX_VALUE;
	//TODO: rate information, complicated by the fact that rates might differ
	//between channels (e.g., weighted-roundrobin splitter)
	public abstract void work();
	/**
	 * Returns the number of output channels this Splitter instance may output
	 * to, or UNLIMITED if there is no limit.
	 * TODO: maybe this should be part of the rate handling?
	 * @return
	 */
	public abstract int outputs();

	//These don't actually do anything, they're just for the JIT to pattern-match against.
	protected final I pop() {return null;};
	protected final I peek(int position) {return null;};
	protected final void push(int channel, O item) {};
}
