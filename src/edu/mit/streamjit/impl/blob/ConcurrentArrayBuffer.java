package edu.mit.streamjit.impl.blob;

import edu.mit.streamjit.util.ConcurrentPeekableQueue;

/**
 * A Buffer implementation based on a lock-free queue.  This implementation
 * does not block.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 7/18/2013
 */
public class ConcurrentArrayBuffer implements Buffer {
	private final ConcurrentPeekableQueue<Object> queue;
	public ConcurrentArrayBuffer(int capacity) {
		this.queue = new ConcurrentPeekableQueue<>(capacity);
	}

	@Override
	public Object read() {
		return queue.poll();
	}

	@Override
	public int read(Object[] data, int offset, int length) {
		int read = 0;
		Object obj;
		while (read < length && (obj = queue.poll()) != null)
			data[offset++] = obj;
		return read;
	}

	@Override
	public boolean readAll(Object[] data) {
		return readAll(data, 0);
	}

	@Override
	public boolean readAll(Object[] data, int offset) {
		int required = data.length - offset;
		if (required > size())
			return false;
		for (; offset < data.length; ++offset) {
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

	@Override
	public int write(Object[] data, int offset, int length) {
		int written = 0;
		while (written < length && queue.offer(data[offset++]))
			++written;
		return written;
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
