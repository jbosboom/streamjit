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
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * A Channel implementation wrapping an arbitrary Queue implementation.  Note
 * that most queues do not support efficient peeking, and some queues do not
 * support efficient size().  This implementation performs no synchronization,
 * but the underlying queue implementation might.
 *
 * This channel uses the add() and remove() methods on the wrapped queue, not
 * offer() and poll(), because they throw the correct exceptions on their own.
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/3/2013
 */
public class QueueChannel<E> implements Channel<E> {
	private final Queue<E> queue;
	public QueueChannel(Queue<E> queue) {
		if (queue == null)
			throw new NullPointerException();
		this.queue = queue;
	}

	@Override
	public void push(E element) {
		queue.add(element);
	}

	@Override
	public E peek(int index) {
		//Best we can do.  Note that the iterator will throw
		//NoSuchElementException, but Channel documents that an
		//IndexOutOfBoundsException will be thrown, so we must translate.
		Iterator<E> iterator = queue.iterator();
		try {
			while (index-- > 0)
				iterator.next();
			return iterator.next();
		} catch (NoSuchElementException ex) {
			IndexOutOfBoundsException nex = new IndexOutOfBoundsException();
			nex.initCause(ex);
			throw nex;
		}
	}

	@Override
	public E pop() {
		return queue.remove();
	}

	@Override
	public int size() {
		return queue.size();
	}

	@Override
	public boolean isEmpty() {
		return queue.isEmpty();
	}

	@Override
	public Iterator<E> iterator() {
		return queue.iterator();
	}
}
