package edu.mit.streamjit.impl.interp;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An EmptyChannel is a capacity-bounded channel with a capacity of 0; that is,
 * it is always empty.
 * @param <E> the type of elements (not) in this channel
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/8/2013
 */
public final class EmptyChannel<E> implements Channel<E> {
	@Override
	public void push(E element) {
		throw new IllegalStateException();
	}

	@Override
	public E peek(int index) {
		throw new IndexOutOfBoundsException();
	}

	@Override
	public E pop() {
		throw new NoSuchElementException();
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return true;
	}

	@Override
	public Iterator<E> iterator() {
		return Collections.emptyIterator();
	}
}
