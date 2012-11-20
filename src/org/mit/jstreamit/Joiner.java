package org.mit.jstreamit;

import java.util.List;

/**
 * see comments on Splitter
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public abstract class Joiner<I, O> extends PrimitiveWorker<I, O> {
	public static final int UNLIMITED = Integer.MAX_VALUE;

	@Override
	protected abstract List<Rate> getPeekRates();
	@Override
	protected abstract List<Rate> getPopRates();
	@Override
	protected abstract List<Rate> getPushRates();

	@Override
	public abstract void work();

	/**
	 * Returns a deep copy of this object.  After this method returns, calls to
	 * other methods on this object have no effect on the returned object, and
	 * vice versa.  Additionally, even for stateless objects, a different object
	 * must be returned (that is, for all x, x != x.copy()).
	 *
	 * Implementations should refine the return type of this method; that is,
	 * myJoiner.copy() should return a MyJoiner rather than just a
	 * Joiner.
	 *
	 * Implementation note: Cloneable is fraught with peril (see Josh Bloch's
	 * Effective Java, Second Edition, Item 11), and the standard replacement of
	 * a copy constructor or static method doesn't work here because we need a
	 * copy with the same dynamic type as this object, thus we need an instance
	 * method.
	 * @return a deep copy of this object
	 */
	public abstract Joiner<I, O> copy();

	/**
	 * Returns the number of input channels this Joiner instance may read
	 * from, or UNLIMITED if any number >= 1 is supported.
	 * TODO: maybe this should be part of the rate handling?
	 * @return
	 */
	public abstract int supportedInputs();

	/**
	 * Returns the number of input channels connected to this joiner.
	 *
	 * This method should only be called from work(), getPeekRates() and
	 * getPopRates() or functions called from them.
	 *
	 * Implementation note: this is a JIT hook method.
	 * @return the number of output channels connected to this splitter
	 */
	protected final int inputs() {
		return getInputChannels().size();
	};

	/**
	 * Peeks at the item at the given position on the given input channel. The index
	 * is 0-based and moves with calls to pop(); that is, peek(i, 0) == pop(i) no
	 * matter how many times pop() is called.
	 *
	 * This method should only be called from work() or functions called from
	 * work().
	 *
	 * Implementation note: this is a JIT hook method.
	 * @param channel the index of the input channel to peek at
	 * @param position the position to peek at
	 * @return an item on the input channel
	 */
	protected final I peek(int channel, int position) {
		//TODO: check rates?
		return getInputChannels().get(channel).peek(position);
	};

	/**
	 * Pops an item off the given input channel.
	 *
	 * This method should only be called from work() or functions called from
	 * work().
	 *
	 * Implementation note: this is a JIT hook method.
	 * @param channel the index of the input channel to pop from
	 * @return the first item in the input channel
	 */
	protected final I pop(int channel) {
		//TODO: check rates?
		return getInputChannels().get(channel).pop();
	};

	/**
	 * Pushes the given item onto the output channel.
	 *
	 * This method should only be called from work() or functions called from
	 * work().
	 *
	 * Implementation note: this is a JIT hook method.
	 * @param item the item to push
	 */
	protected final void push(O item) {
		//TODO: check rates?
		getOutputChannels().get(0).push(item);
	};
}
