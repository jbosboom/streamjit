package org.mit.jstreamit;

import java.util.Iterator;

/**
 * Channel represents a communication channel between two primitive workers in
 * the stream graph, providing methods to push, pop and peek at elements in the
 * channel.
 *
 * @param <E> the type of elements in this channel
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/2/2013
 */
public interface Channel<E> extends Iterable<E> {
	/**
	 * Adds the given element to the end of this channel. Updates the push
	 * count.
	 * @param element the element to add
	 */
	public void push(E element);

	/**
	 * Returns the element at the given index. Updates the maximum peek index.
	 * @param index the index to peek at
	 * @return the element at the given index
	 * @throws IndexOutOfBoundsException if index < 0 or index > size()
	 */
	public E peek(int index);

	/**
	 * Removes and returns the element at the front of this channel.  Updates
	 * the pop count.
	 * @return the element at the front of this channel
	 * @throws NoSuchElementException if this channel is empty
	 */
	public E pop();

	/**
	 * Returns this channel's logical size: the number of elements that can be
	 * popped.
	 * @return this channel's logical size
	 */
	public int size();

	/**
	 * Returns true iff this channel contains no elements.
	 * @return true iff this channel contains no elements.
	 */
	public boolean isEmpty();

	/**
	 * Returns an iterator that iterates over the elements in this channel in
	 * first-to-last order.  The returned iterator may or may not support
	 * remove().
	 * @return an iterator over this channel
	 */
	@Override
	public Iterator<E> iterator();
}
