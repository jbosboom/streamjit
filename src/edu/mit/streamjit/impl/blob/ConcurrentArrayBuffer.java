package edu.mit.streamjit.impl.blob;

import edu.mit.streamjit.util.ConcurrentPeekableQueue;

/**
 * A Buffer implementation based on a lock-free queue.  This implementation
 * does not block.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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
