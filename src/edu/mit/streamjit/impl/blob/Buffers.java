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

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * Contains static methods related to Buffer instances.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 7/28/2013
 */
public final class Buffers {
	private Buffers() {}

	/**
	 * Returns a Buffer backed by the given queue. The returned buffer reports
	 * the given capacity, but does not enforce it (though the queue might, and
	 * clients will usually try to respect it).
	 * <p/>
	 * If the given queue is not empty, the Buffer will initially contain the
	 * queue's contents. Modifying the queue after the Buffer has been created
	 * may yield strange results or exceptions.
	 * <p/>
	 * The returned Buffer implementation does not block, as Queue does not
	 * provide blocking methods.
	 * <p/>
	 * The returned Buffer performs no synchronization of its own, thus
	 * inheriting the queue's synchronization policy.
	 * @param queue the queue to use
	 * @param capacity the queue's capacity
	 * @return a Buffer backed by the given queue
	 */
	public static Buffer queueBuffer(final Queue<Object> queue, final int capacity) {
		return new AbstractBuffer() {
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
				return capacity;
			}
		};
	}

	/**
	 * Returns a Buffer backed by the given BlockingQueue.
	 * <p/>
	 * If the given queue is not empty, the Buffer will initially contain the
	 * queue's contents. Modifying the queue after the Buffer has been created
	 * may yield strange results or exceptions.
	 * <p/>
	 * The returned Buffer implementation's blocking behavior can be controlled
	 * separately for readers and writers. Even if readers block, readAll() may
	 * still fail (returning false) as the BlockingQueue interface does not
	 * support atomic bulk removes.
	 * <p/>
	 * The returned Buffer performs no synchronization of its own, thus
	 * inheriting the queue's synchronization policy.
	 * @param queue the queue to use
	 * @param readerBlocks true iff reads should block
	 * @param writerBlocks true iff writes should block
	 * @return a Buffer backed by the given queue
	 */
	public static Buffer blockingQueueBuffer(final BlockingQueue<Object> queue, boolean readerBlocks, boolean writerBlocks) {
		int remainingCapacity = queue.remainingCapacity();
		final int capacity = remainingCapacity == Integer.MAX_VALUE ? remainingCapacity : remainingCapacity + queue.size();
		if (!readerBlocks && !writerBlocks)
			return queueBuffer(queue, capacity);

		abstract class AbstractBlockingQueueBuffer extends AbstractBuffer {
			@Override
			public int size() {
				return queue.size();
			}
			@Override
			public int capacity() {
				return capacity;
			}
		}

		if (readerBlocks && !writerBlocks) {
			return new AbstractBlockingQueueBuffer() {
				@Override
				public Object read() {
					try {
						return queue.take();
					} catch (InterruptedException ex) {
						return null;
					}
				}
				@Override
				public boolean readAll(Object[] data, int offset) {
					int required = data.length - offset;
					if (required > size())
						return false;
					for (; offset < data.length; ++offset) {
						//We poll, even though we're specified to block on
						//reads, because we need to ignore interrupts once we're
						//committed to the atomic readAll.
						Object e = queue.poll();
						//We checked size() above, so we should never fail here, except in
						//case of concurrent modification by another reader.
						assert e != null;
						data[offset] = e;
					}
					return true;
				}
				@Override
				public boolean write(Object t) {
					return queue.offer(t);
				}
			};
		} else if (!readerBlocks && writerBlocks) {
			return new AbstractBlockingQueueBuffer() {
				@Override
				public Object read() {
					return queue.poll();
				}
				@Override
				public boolean write(Object t) {
					try {
						queue.put(t);
						return true;
					} catch (InterruptedException ex) {
						return false;
					}
				}
			};
		} else {
			return new AbstractBlockingQueueBuffer() {
				@Override
				public Object read() {
					try {
						return queue.take();
					} catch (InterruptedException ex) {
						return null;
					}
				}
				@Override
				public boolean readAll(Object[] data, int offset) {
					int required = data.length - offset;
					if (required > size())
						return false;
					for (; offset < data.length; ++offset) {
						//We poll, even though we're specified to block on
						//reads, because we need to ignore interrupts once we're
						//committed to the atomic readAll.
						Object e = queue.poll();
						//We checked size() above, so we should never fail here, except in
						//case of concurrent modification by another reader.
						assert e != null;
						data[offset] = e;
					}
					return true;
				}
				@Override
				public boolean write(Object t) {
					try {
						queue.put(t);
						return true;
					} catch (InterruptedException ex) {
						return false;
					}
				}
			};
		}
	}

	/**
	 * Returns a read-only view of the given buffer.  (Note that reads still
	 * modify the buffer as usual; this wrapper merely prohibits the write
	 * methods.)
	 * @param buffer the buffer to wrap
	 * @return a read-only view of the given buffer
	 */
	public static Buffer readOnlyBuffer(final Buffer buffer) {
		checkNotNull(buffer);
		return new Buffer() {
			@Override
			public Object read() {
				return buffer.read();
			}
			@Override
			public int read(Object[] data, int offset, int length) {
				return buffer.read(data, offset, length);
			}
			@Override
			public boolean readAll(Object[] data) {
				return buffer.readAll(data);
			}
			@Override
			public boolean readAll(Object[] data, int offset) {
				return buffer.readAll(data, offset);
			}
			@Override
			public boolean write(Object t) {
				throw new UnsupportedOperationException("read-only buffer");
			}
			@Override
			public int write(Object[] data, int offset, int length) {
				throw new UnsupportedOperationException("read-only buffer");
			}
			@Override
			public int size() {
				return buffer.size();
			}
			@Override
			public int capacity() {
				return buffer.capacity();
			}
		};
	}
}
