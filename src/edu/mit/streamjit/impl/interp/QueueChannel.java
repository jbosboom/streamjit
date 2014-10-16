package edu.mit.streamjit.impl.interp;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * A Channel implementation wrapping an arbitrary Queue implementation.  Note
 * that most queues do not support efficient peeking, and some queues do not
 * support efficient size().  This implementation performs no synchronization,
 * but the underlying queue implementation might.
 *
 * This channel uses the add() and remove() methods on the wrapped queue, not
 * offer() and poll(), because they throw the correct exceptions on their own.
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/3/2013
 */
public class QueueChannel<E> implements Channel<E> {
	private final Queue<E> queue;
	public QueueChannel(Queue<E> queue) {
		if (queue == null)
			throw new NullPointerException();
		this.queue = queue;
	}

	@Override
	public void push(E element) {
		queue.add(element);
	}

	@Override
	public E peek(int index) {
		//Best we can do.  Note that the iterator will throw
		//NoSuchElementException, but Channel documents that an
		//IndexOutOfBoundsException will be thrown, so we must translate.
		Iterator<E> iterator = queue.iterator();
		try {
			while (index-- > 0)
				iterator.next();
			return iterator.next();
		} catch (NoSuchElementException ex) {
			IndexOutOfBoundsException nex = new IndexOutOfBoundsException();
			nex.initCause(ex);
			throw nex;
		}
	}

	@Override
	public E pop() {
		return queue.remove();
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
}
