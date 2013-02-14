package edu.mit.streamjit;

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
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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
