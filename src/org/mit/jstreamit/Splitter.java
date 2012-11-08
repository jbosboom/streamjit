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
	 * to, or UNLIMITED if any number >= 1 is supported.
	 * TODO: maybe this should be part of the rate handling?
	 * @return
	 */
	public abstract int supportedOutputs();

	//These don't actually do anything, they're just for the JIT to
	//pattern-match against.  We can't make them abstract because they're final,
	//but they have to be final so implementations don't think they
	//can customize them.
	protected final I pop() {return null;};
	protected final I peek(int position) {return null;};
	protected final void push(int channel, O item) {};
	protected final int outputs() {return 0;};
}
