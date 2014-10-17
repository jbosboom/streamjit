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
package edu.mit.streamjit.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A lock-free bounded FIFO queue based on a circular array, supporting either
 * one consumer and unlimited producers, or one producer and unlimited
 * consumers.  This queue does not accept nulls.
 *
 * See "A practical nonblocking queue algorithm using compare-and-swap", Shann,
 * Huang, and Chen.  We provide size() and peek(int) using the fact that only
 * consumers modify front, so if there's only one consumer, it can peek without
 * worrying about elements being removed from underneath it.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/2/2013
 */
public class ConcurrentPeekableQueue<E> {
	private final AtomicReferenceArray<E> elements;
	private final AtomicLong front, rear;
	public ConcurrentPeekableQueue(int maxSize) {
		if (maxSize <= 1)
			throw new IllegalArgumentException("Size too small:"+ maxSize);
		this.elements = new AtomicReferenceArray<>(maxSize);
		this.front = new AtomicLong(0);
		this.rear = new AtomicLong(0);
	}

	/**
	 * Inserts the given element if size permits.
	 * @param element the element to insert
	 * @return true iff the element was inserted
	 */
	public boolean offer(E element) {
		if (element == null)
			throw new NullPointerException();
		while (true) {
			long r = rear.get();
			int i = (int)(r % elements.length());
			E x = elements.get(i);
			//Did rear change while we were reading x?
			if (r != rear.get())
				continue;
			//Is the queue full?
			if (r == front.get() + elements.length())
				return false; //Don't retry; fail the offer.

			if (x == null) {//Is the rear empty?
				if (elements.compareAndSet(i, x, element)) {//Try to store an element.
					//Try to increment rear.  If we fail, other threads will
					//also try to increment before any further insertions, so we
					//don't need to loop.
					rear.compareAndSet(r, r+1);
					return true;
				}
			} else //rear not empty.  Try to help other threads.
				rear.compareAndSet(r, r+1);

			//If we get here, we failed at some point.  Try again.
		}
	}

	/**
	 * Removes an element if there is one, or returns null.
	 * @return an element, or null
	 */
	public E poll() {
		while (true) {
			long f = front.get();
			int i = (int)(f % elements.length());
			E x = elements.get(i);
			//Did front change while we were reading x?
			if (f != front.get())
				continue;
			//Is the queue empty?
			if (f == rear.get())
				return null; //Don't retry; fail the poll.

			if (x != null) {//Is the front nonempty?
				if (elements.compareAndSet(i, x, null)) {//Try to remove an element.
					//Try to increment front.  If we fail, other threads will
					//also try to increment before any further removals, so we
					//don't need to loop.
					front.compareAndSet(f, f+1);
					return x;
				}
			} else //front empty.  Try to help other threads.
				front.compareAndSet(f, f+1);

			//If we get here, we failed at some point.  Try again.
		}
	}

	/**
	 * Returns the element at the given index from the front of the queue, or
	 * null if there aren't that many elements.  Use this method only when only
	 * one consumer is using this queue.
	 * @param index the element to peek at
	 * @return the element at the given index, or null
	 */
	public E peek(int index) {
		//Because we're the only consumer, we know nothing will be removed while
		//we're peeking.  So we check we have enough elements, then go get what
		//we want.
		if (index < size())
			return elements.get((int)((front.get() + index) % elements.length()));
		return null;
	}

	/**
	 * Returns a lower bound of the number of elements in this queue.  The
	 * actual number may be larger, but will never be lower.  Use this method
	 * only when only one consumer is using this queue.
	 * @return a lower bound of this queue's size
	 */
	public int size() {
		//Because we're the only consumer, we know nothing will be removed while
		//we're computing the size, so we know there are at least (rear - front)
		//elements already added.
		return (int)(rear.get() - front.get());
	}

	/**
	 * Returns the maximum number of elements that can be in this queue.  This
	 * value will never change during the life of the queue.
	 * @return this queue's capacity
	 */
	public int capacity() {
		return elements.length();
	}

	/**
	 * Returns an iterator over the elements of this queue, front to back.
	 * The returned iterator's behavior is undefined if this queue is modified
	 * while iteration is in progress.
	 * @return an iterator over this queue
	 */
	public Iterator<E> iterator() {
		final int begin = (int)(front.get() % elements.length()), end = (int)(rear.get() % elements.length());
		return new Iterator<E>() {
			private int i = begin;
			private boolean removed = false;
			@Override
			public boolean hasNext() {
				return i < end;
			}
			@Override
			public E next() {
				if (!hasNext())
					throw new NoSuchElementException();
				removed = false;
				return elements.get(i++);
			}
			@Override
			public void remove() {
				//TODO: this could work if we only remove items from the front.
				throw new UnsupportedOperationException();
//				if (i == begin || removed)
//					throw new IllegalStateException();
//				elements.set(i-1, null);
//				removed = true;
			}
		};
	}

	public static void main(String[] args) {
		ConcurrentPeekableQueue<Integer> cpq = new ConcurrentPeekableQueue<>(40);
		cpq.offer(0);
		cpq.offer(1);
		cpq.offer(2);
		System.out.println(cpq.size());
		System.out.println(cpq.iterator().next());
		System.out.println(cpq.poll());
	}
}
