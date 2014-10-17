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

import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.impl.common.Workers;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/7/2012
 */
public abstract class Filter<I, O> extends Worker<I, O> implements OneToOneElement<I, O> {
	private final Rate peekRate, popRate, pushRate;

	/**
	 * Creates a new Filter object with the given pop and push rates and a peek
	 * rate of 0.
	 * @param popRate the pop rate
	 * @param pushRate the push rate
	 */
	public Filter(int popRate, int pushRate) {
		//TODO: should we pass pushRate for peekRate instead of 0?
		this(popRate, pushRate, 0);
	}

	/**
	 * Creates a new Filter object with the given pop, push and peek rates.
	 * @param popRate the pop rate
	 * @param pushRate the push rate
	 * @param peekRate the peek rate
	 */
	public Filter(int popRate, int pushRate, int peekRate) {
		this(Rate.create(popRate), Rate.create(pushRate), Rate.create(peekRate));
	}

	/**
	 * Creates a new Filter object with the given pop, push and peek rates.
	 * @param popRate the pop rate
	 * @param pushRate the push rate
	 * @param peekRate the peek rate
	 */
	public Filter(Rate popRate, Rate pushRate, Rate peekRate) {
		if (popRate == null || pushRate == null || peekRate == null)
			throw new IllegalArgumentException(
					String.format("pop %s push %s peek %s",
					popRate, pushRate, peekRate));
		this.popRate = popRate;
		this.pushRate = pushRate;
		this.peekRate = peekRate;
	}

	public abstract void work();

	@Override
	public final void visit(StreamVisitor v) {
		v.visitFilter0(this);
	}

	@Override
	public final ImmutableList<Rate> getPeekRates() {
		return ImmutableList.of(peekRate);
	}

	@Override
	public final ImmutableList<Rate> getPopRates() {
		return ImmutableList.of(popRate);
	}

	@Override
	public final ImmutableList<Rate> getPushRates() {
		return ImmutableList.of(pushRate);
	}

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
