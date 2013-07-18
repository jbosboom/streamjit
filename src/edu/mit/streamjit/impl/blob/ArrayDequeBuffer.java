package edu.mit.streamjit.impl.blob;

import java.util.ArrayDeque;

/**
 * An unsynchronized, unbounded Buffer implementation based on an ArrayDeque.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 7/18/2013
 */
public class ArrayDequeBuffer implements Buffer {
	private final ArrayDeque<Object> deque;
	public ArrayDequeBuffer() {
		this.deque = new ArrayDeque<>();
	}
	public ArrayDequeBuffer(int initialCapacityHint) {
		this.deque = new ArrayDeque<>(initialCapacityHint);
	}

	@Override
	public Object read() {
		return deque.poll();
	}

	@Override
	public int read(Object[] data, int offset, int length) {
		int read = 0;
		Object obj;
		while (read < length && (obj = deque.poll()) != null)
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
			Object e = deque.poll();
			//We checked size() above, so we should never fail here, except in
			//case of concurrent modification by another reader.
			assert e != null;
			data[offset] = e;
		}
		return true;
	}

	@Override
	public boolean write(Object t) {
		return deque.offer(t);
	}

	@Override
	public int write(Object[] data, int offset, int length) {
		int written = 0;
		while (written < length && deque.offer(data[offset++]))
			++written;
		return written;
	}

	@Override
	public int size() {
		return deque.size();
	}

	@Override
	public int capacity() {
		return Integer.MAX_VALUE;
	}
}
