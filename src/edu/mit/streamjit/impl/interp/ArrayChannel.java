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

import com.google.common.collect.Iterators;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * ArrayChannel is a Channel implementation backed by a resizable array,
 * analogous to ArrayList.  This implementation is not synchronized.
 *
 * @param <E> the type of elements in this channel
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/19/2012
 */
public class ArrayChannel<E> implements Channel<E> {
	/**
	 * The default buffer capacity.  Most Java collections choose 16, at least
	 * in the JRE I'm looking at, but we might want something different.
	 */
	private static final int DEFAULT_CAPACITY = 16;
	/**
	 * The growth factor for reallocating the underlying array.  Java
	 * collections vary between 1.5 and 2.
	 */
	private static final double GROWTH_FACTOR = 2;
	/**
	 * The buffer holding elements in the channel.  Elements [head, tail) are
	 * in the channel; other elements are null.  Note that this buffer is
	 * managed circularly, and at least one element is empty before and after
	 * each method executes.
	 */
	private E[] buffer;
	/**
	 * The index of the first element in this channel (the element returned by
	 * pop() or peek(0)), or an arbitrary number equal to tail if the channel
	 * is empty.
	 */
	private int head;
	/**
	 * One greater than the index of the last element in this channel (that is,
	 * the index where an element would be added by push()), or an arbitrary
	 * number equal to head if the channel is empty.
	 */
	private int tail;

	/**
	 * Constructs an empty ArrayChannel with the default capacity.
	 */
	public ArrayChannel() {
		this(DEFAULT_CAPACITY);
	}

	/**
	 * Constructs an empty ArrayChannel with the given initial capacity.
	 * @param capacity the new ArrayChannel's initial capacity
	 */
	@SuppressWarnings("unchecked")
	public ArrayChannel(int capacity) {
		if (capacity < 0)
			throw new IllegalArgumentException();
		capacity = Math.min(capacity, DEFAULT_CAPACITY);
		buffer = (E[])new Object[capacity+1];
	}

	/**
	 * Constructs a ArrayChannel containing the given elements.  The elements are
	 * copied into the channel; the given array is not modified.
	 * @param initialElements the initial elements
	 */
	public ArrayChannel(E[] initialElements) {
		this(Arrays.asList(initialElements));
	}

	/**
	 * Constructs a ArrayChannel containing the given elements.  The elements are
	 * copied into the channel; the given collection is not modified.
	 * @param initialElements the initial elements
	 */
	public ArrayChannel(Collection<? extends E> initialElements) {
		this(initialElements.size() + 1);
		for (E e : initialElements)
			buffer[tail++] = e;
	}

	/* Basic operations */

	@Override
	public E peek(int index) {
		if (index < 0 || index > size())
			throw new IndexOutOfBoundsException(String.format("index %d, size %d", index, size()));
		//Compute the physical index.
		int physicalIndex = head + index;
		if (physicalIndex >= buffer.length)
			physicalIndex -= buffer.length;
		return buffer[physicalIndex];
	}

	@Override
	public void push(E element) {
		if (head == increment(tail))
			expandCapacity();
		buffer[tail] = element;
		tail = increment(tail);
	}

	@Override
	public E pop() {
		if (isEmpty())
			throw new NoSuchElementException("Channel is empty");
		E retval = buffer[head];
		//Help the garbage collector recognize this element as garbage.
		buffer[head] = null;
		head = increment(head);
		return retval;
	}

	@Override
	public int size() {
		int size = tail - head;
		if (size < 0)
			size += buffer.length;
		return size;
	}

	@Override
	public boolean isEmpty()  {
		return head == tail;
	}

	/* Transitioning to other representations (compiler, debugging) */

	/**
	 * Returns the current capacity of this channel (the number of elements it
	 * can hold without triggering a reallocation).
	 * @return the channel's capacity
	 */
	public int getCapacity() {
		return buffer.length - 1;
	}

	public void ensureCapacity(int capacity) {
		if (capacity > getCapacity())
			setLength(capacity+1);
	}

	/**
	 * Trims the capacity of this channel to its current size, to reduce memory
	 * consumption.
	 */
	public void trimToSize() {
		if (isEmpty())
			setLength(2);
		else
			setLength(size()+1);
	}

	@Override
	public Iterator<E> iterator() {
		return new Iterator<E>() {
			private int position = head;
			@Override
			public boolean hasNext() {
				return position != tail;
			}
			@Override
			public E next() {
				if (!hasNext())
					throw new NoSuchElementException();
				E retval = buffer[position];
				position = increment(position);
				return retval;
			}
			@Override
			public void remove() {
				//We could support remove() for a prefix of the elements
				//returned by next() (i.e., once you call next() twice in a row,
				//you can't remove() anymore).
				throw new UnsupportedOperationException("Not supported.");
			}
		};
	}

	@Override
	public String toString() {
		return Iterators.toString(iterator());
	}

	/* Helper methods */

	/**
	 * Returns the index logically one greater than the given index, wrapping
	 * around the buffer size as required.
	 * @param index the index to increment
	 * @return the next index
	 */
	private int increment(int index) {
		++index;
		//TODO: we could trade the branch for a division by using % instead.
		if (index == buffer.length)
			index = 0;
		return index;
	}

	/**
	 * Grow the buffer based on the growth factor.
	 */
	private void expandCapacity() {
		int oldLength = buffer.length;
		int newLength = (int)Math.ceil(oldLength * GROWTH_FACTOR);
		if (newLength <= 0)
			newLength = Integer.MAX_VALUE; //Good luck allocating that much...
		assert newLength >= buffer.length;
		setLength(newLength);
	}

	/**
	 * Sets the circular buffer's length.  Note that capacity is one less than
	 * the length.
	 * @param newLength the new buffer length
	 */
	private void setLength(int newLength) {
		int size = size();
		assert newLength >= size + 1;
		int oldLength = buffer.length;
		@SuppressWarnings("unchecked")
		E[] newBuffer = (E[])new Object[newLength];
		//Copy over the elements in the correct order.
		if (tail >= head)
			System.arraycopy(buffer, head, newBuffer, 0, size);
		else {
			System.arraycopy(buffer, head, newBuffer, 0, oldLength-head);
			System.arraycopy(buffer, 0, newBuffer, oldLength-head, tail);
		}
		head = 0;
		tail = size;
		buffer = newBuffer;
		assert size() == size;
	}
}
