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

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.util.SneakyThrows;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

/**
 * A thread-safe Channel implementation based on a BlockingQueue.  Calls to push
 * and pop block until data is available.  InterruptedExceptions are thrown
 * despite Channel's methods not allowing checked exceptions.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/22/2013
 */
public class BlockingQueueChannel<E> implements Channel<E> {
	private final BlockingQueue<E> queue;
	public BlockingQueueChannel(BlockingQueue<E> queue) {
		this.queue = checkNotNull(queue);
	}

	@Override
	public void push(E element) {
		try {
			queue.put(element);
		} catch (InterruptedException ex) {
			throw SneakyThrows.sneakyThrow(ex);
		}
	}

	@Override
	public E peek(int index) {
		return Iterables.get(queue, index);
	}

	@Override
	public E pop() {
		try {
			return queue.take();
		} catch (InterruptedException ex) {
			throw SneakyThrows.sneakyThrow(ex);
		}
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

	@Override
	public String toString() {
		return queue.toString();
	}
}
