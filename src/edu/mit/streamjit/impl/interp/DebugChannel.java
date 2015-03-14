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
package edu.mit.streamjit.impl.interp;

import java.util.Collection;

/**
 * A Channel implementation, based on ArrayChannel, with additional methods for
 * debugging.  This implementation is not synchronized.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/2/2013
 */
public final class DebugChannel<E> extends ArrayChannel<E> {
	/**
	 * The number of calls to push() since the last call to resetStatistics().
	 */
	private int pushCount;
	/**
	 * The number of calls to pop() since the last call to resetStatistics().
	 */
	private int popCount;
	/**
	 * The maximum index passed to peek() since the last call to
	 * resetStatistics(), relative to the position of head at the time of that
	 * last call to resetStatistics(). That is, a number useful for checking
	 * peek rate declarations.
	 */
	private int maxPeekIndex;

	/**
	 * Constructs an empty DebugChannel with the default capacity.
	 */
	public DebugChannel() {
		super();
		resetStatistics();
	}

	/**
	 * Constructs an empty DebugChannel with the given initial capacity.
	 * @param capacity the new DebugChannel's initial capacity
	 */
	public DebugChannel(int capacity) {
		super(capacity);
		resetStatistics();
	}

	/**
	 * Constructs a DebugChannel containing the given elements.  The elements are
	 * copied into the channel; the given array is not modified.
	 * @param initialElements the initial elements
	 */
	public DebugChannel(E[] initialElements) {
		super(initialElements);
		resetStatistics();
	}

	/**
	 * Constructs a DebugChannel containing the given elements.  The elements are
	 * copied into the channel; the given collection is not modified.
	 * @param initialElements the initial elements
	 */
	public DebugChannel(Collection<? extends E> initialElements) {
		super(initialElements);
		resetStatistics();
	}

	@Override
	public void push(E element) {
		super.push(element);
		++pushCount;
	}

	@Override
	public E peek(int index) {
		E e = super.peek(index);
		maxPeekIndex = Math.max(maxPeekIndex, index + popCount);
		return e;
	}

	@Override
	public E pop() {
		E e = super.pop();
		++popCount;
		return e;
	}

	/**
	 * Returns the maximum index passed to peek() since the last call to
	 * resetStatistics(), relative to the front of this channel at the time of
	 * that last call.  If no peeking occurred, returns -1.
	 * @return the maximum peek index
	 */
	public int getMaxPeekIndex() {
		return maxPeekIndex;
	}

	/**
	 * Returns the number of calls to push() since the last call to
	 * resetStatistics().
	 * @return the push count
	 */
	public int getPushCount() {
		return pushCount;
	}

	/**
	 * Returns the number of calls to pop() since the last call to
	 * resetStatistics().
	 * @return the pop count
	 */
	public int getPopCount() {
		return popCount;
	}

	/**
	 * Resets this channel's statistics (push count, pop count and max peek
	 * index).
	 */
	public void resetStatistics() {
		popCount = 0;
		pushCount = 0;
		maxPeekIndex = -1;
	}
}
