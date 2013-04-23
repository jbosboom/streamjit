package edu.mit.streamjit.api;

import edu.mit.streamjit.impl.common.Workers;

/**
 * TODO: splitters with multiple output types?  would sacrifice CTTS.
 * Splitter is an abstract class rather than an interface to allow the library
 * to handle rate info management.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public abstract class Splitter<I> extends Worker<I, I> {
	public static final int UNLIMITED = Integer.MAX_VALUE;

	/**
	 * Returns the number of output channels this Splitter instance may output
	 * to, or UNLIMITED if any number >= 1 is supported.
	 * TODO: maybe this should be part of the rate handling?
	 * @return
	 */
	public abstract int supportedOutputs();

	@Override
	public void visit(StreamVisitor v) {
		v.visitSplitter(this);
	}

	/**
	 * Returns the number of output channels connected to this splitter.
	 *
	 * This method should only be called from work() and getPushRates() or
	 * functions called from them.
	 *
	 * Implementation note: this is a JIT hook method.
	 * @return the number of output channels connected to this splitter
	 */
	protected final int outputs() {
		return Workers.getOutputChannels(this).size();
	};

	/**
	 * Peeks at the item at the given position on the input channel. The index
	 * is 0-based and moves with calls to pop(); that is, peek(0) == pop() no
	 * matter how many times pop() is called.
	 *
	 * This method should only be called from work() or functions called from
	 * work().
	 *
	 * Implementation note: this is a JIT hook method.
	 * @param position the position to peek at
	 * @return an item on the input channel
	 */
	protected final I peek(int position) {
		return Workers.getInputChannels(this).get(0).peek(position);
	};

	/**
	 * Pops an item off the input channel.
	 *
	 * This method should only be called from work() or functions called from
	 * work().
	 *
	 * Implementation note: this is a JIT hook method.
	 * @return the first item in the input channel
	 */
	protected final I pop() {
		return Workers.getInputChannels(this).get(0).pop();
	};

	/**
	 * Pushes the given item onto the given output channel.
	 *
	 * This method should only be called from work() or functions called from
	 * work().
	 *
	 * Implementation note: this is a JIT hook method.
	 * @param channel the index of the output channel to push onto
	 * @param item the item to push
	 */
	protected final void push(int channel, I item) {
		Workers.getOutputChannels(this).get(channel).push(item);
	};
}
