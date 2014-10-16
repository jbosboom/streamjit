package edu.mit.streamjit.impl.interp;

import java.util.Iterator;

/**
 * A Channel implementation that delegates to another Channel implementation,
 * using a lock to synchronize all its methods.  Note that this class' iterator
 * is not itself synchronized (and may or may not throw
 * ConcurrentModificationException depending on the underlying implementation).
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/8/2013
 */
public class SynchronizedChannel<E> implements Channel<E>{
	private final Channel<E> delegate;
	public SynchronizedChannel(Channel<E> delegate) {
		if (delegate == null)
			throw new NullPointerException();
		this.delegate = delegate;
	}

	@Override
	public synchronized void push(E element) {
		delegate.push(element);
	}

	@Override
	public synchronized E peek(int index) {
		return delegate.peek(index);
	}

	@Override
	public synchronized E pop() {
		return delegate.pop();
	}

	@Override
	public synchronized int size() {
		return delegate.size();
	}

	@Override
	public synchronized boolean isEmpty() {
		return delegate.isEmpty();
	}

	@Override
	public synchronized Iterator<E> iterator() {
		return delegate.iterator();
	}

	@Override
	public synchronized String toString() {
		return delegate.toString();
	}
}
