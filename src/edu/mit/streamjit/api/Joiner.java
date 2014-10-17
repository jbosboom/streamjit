/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.api;

import edu.mit.streamjit.impl.common.Workers;

/**
 * see comments on Splitter
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/7/2012
 */
public abstract class Joiner<I, O> extends Worker<I, O> {
	public static final int UNLIMITED = Integer.MAX_VALUE;

	@Override
	public final void visit(StreamVisitor v) {
		v.visitJoiner(this);
	}

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
		return Workers.getInputChannels(this).size();
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
		return Workers.getInputChannels(this).get(channel).peek(position);
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
		return Workers.getInputChannels(this).get(channel).pop();
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
		Workers.getOutputChannels(this).get(0).push(item);
	};
}
