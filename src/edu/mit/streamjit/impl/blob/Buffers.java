package edu.mit.streamjit.impl.blob;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * Contains static methods related to Buffer instances.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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

	/**
	 * Returns a read-only Buffer view of the given Iterable, in the order
	 * returned by its iterator. Elements removed from the Buffer are *not*
	 * removed from the iterable.
	 * <p/>
	 * This method is intended for generating sample inputs in tests and
	 * benchmarks; for real use, see queueBuffer or blockingQueueBuffer.
	 * @param iterable the iterable to wrap
	 * @return a read-only Buffer view of the given iterable
	 */
	public static Buffer fromIterable(Iterable<?> iterable) {
		return fromIterator(iterable.iterator(), Iterables.size(iterable));
	}

	/**
	 * Returns a read-only Buffer view of the given Iterator, in the order
	 * returned by next(). Elements removed from the Buffer are *not* removed
	 * from the iterator.
	 * <p/>
	 * This method is intended for generating sample inputs in tests and
	 * benchmarks; for real use, see queueBuffer or blockingQueueBuffer.
	 * @param iterator the iterator to wrap
	 * @param size the number of elements in the iterator
	 * @return a read-only Buffer view of the given iterator
	 */
	public static Buffer fromIterator(final Iterator<?> iterator, final int size) {
		return new AbstractBuffer() {
			private int remainingSize = size;
			@Override
			public Object read() {
				if (!iterator.hasNext())
					return null;
				--remainingSize;
				return iterator.next();
			}
			@Override
			public boolean write(Object t) {
				throw new UnsupportedOperationException("read-only buffer");
			}
			@Override
			public int size() {
				return remainingSize;
			}
			@Override
			public int capacity() {
				//This shouldn't matter for a read-only buffer...
				return size();
			}
		};
	}
}
