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

import edu.mit.streamjit.util.ConcurrentPeekableQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An Channel implementation that is safe for use by one consumer and multiple
 * producers or one producer and multiple consumers.  Note that peek(), size()
 * and isEmpty() are only thread-safe when in the one-consumer mode, and
 * iterator() is not safe when the channel is being modified (by any number of
 * threads except zero).
 *
 * Due to the bounded, nonblocking nature of the underlying storage, push(),
 * peek() and pop() may all fail by throwing an exception.  Typically, this
 * channel is used as the first channel in a stream graph, where push() failures
 * are translated into returning false from CompiledStream.offer() and the
 * stream graph thread busy-waits on size() to guarantee peek() and pop() will
 * succeed.
 *
 * Despite the name, this implementation is not related to ArrayChannel.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/3/2013
 */
public class ConcurrentArrayChannel<E> implements Channel<E> {
	private final ConcurrentPeekableQueue<E> queue;
	public ConcurrentArrayChannel(int maxSize) {
		this.queue = new ConcurrentPeekableQueue<>(maxSize);
	}

	@Override
	public void push(E element) {
		if (!queue.offer(element))
			throw new IllegalStateException("channel is full");
	}

	@Override
	public E peek(int index) {
		E e = queue.peek(index);
		if (e == null)
			throw new IndexOutOfBoundsException();
		return e;
	}

	@Override
	public E pop() {
		E e = queue.poll();
		if (e == null)
			throw new NoSuchElementException();
		return e;
	}

	@Override
	public int size() {
		return queue.size();
	}

	@Override
	public boolean isEmpty() {
		return queue.size() == 0;
	}

	@Override
	public Iterator<E> iterator() {
		return queue.iterator();
	}
}
