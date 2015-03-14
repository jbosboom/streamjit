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
package edu.mit.streamjit.impl.blob;

import edu.mit.streamjit.util.ConcurrentPeekableQueue;

/**
 * A Buffer implementation based on a lock-free queue.  This implementation
 * does not block.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 7/18/2013
 */
public class ConcurrentArrayBuffer extends AbstractBuffer {
	private final ConcurrentPeekableQueue<Object> queue;
	public ConcurrentArrayBuffer(int capacity) {
		//ConcurrentPeekableQueue must be at least 2 capacity.
		if (capacity == 1)
			capacity = 2;
		this.queue = new ConcurrentPeekableQueue<>(capacity);
	}

	@Override
	public Object read() {
		return queue.poll();
	}

	@Override
	public boolean write(Object t) {
		return queue.offer(t);
	}

	@Override
	public int size() {
		return queue.size();
	}

	@Override
	public int capacity() {
		return queue.capacity();
	}
}
