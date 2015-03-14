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

import java.util.Iterator;

/**
 * Channel represents a communication channel between two primitive workers in
 * the stream graph, providing methods to push, pop and peek at elements in the
 * channel.
 *
 * @param <E> the type of elements in this channel
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/2/2013
 */
public interface Channel<E> extends Iterable<E> {
	/**
	 * Adds the given element to the end of this channel.
	 * @param element the element to add
	 * @throws IllegalStateException if the element cannot be added at this time
	 * due to capacity restrictions
	 * @throws NullPointerException if the specified element is null and this
	 * channel does not permit null elements
	 */
	public void push(E element);

	/**
	 * Returns the element at the given index.
	 * @param index the index to peek at
	 * @return the element at the given index
	 * @throws IndexOutOfBoundsException if index < 0 or index > size()
	 */
	public E peek(int index);

	/**
	 * Removes and returns the element at the front of this channel.
	 * @return the element at the front of this channel
	 * @throws NoSuchElementException if this channel is empty
	 */
	public E pop();

	/**
	 * Returns this channel's logical size: the number of elements that can be
	 * popped.
	 * @return this channel's logical size
	 */
	public int size();

	/**
	 * Returns true iff this channel contains no elements.
	 * @return true iff this channel contains no elements.
	 */
	public boolean isEmpty();

	/**
	 * Returns an iterator that iterates over the elements in this channel in
	 * first-to-last order.  The returned iterator may or may not support
	 * remove().
	 * @return an iterator over this channel
	 */
	@Override
	public Iterator<E> iterator();
}
