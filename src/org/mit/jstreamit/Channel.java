package org.mit.jstreamit;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Channel represents a communication channel between two primitive workers in
 * the stream graph, providing methods to push, pop and peek at elements in the
 * channel.
 *
 * This class is not thread-safe.
 * @param <E> the type of elements in this channel
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/19/2012
 */
public final class Channel<E> implements Iterable<E> {
	/**
	 * The default buffer capacity.  Most Java collections choose 16, at least
	 * in the JRE I'm looking at, but we might want something different.
	 */
	private static final int DEFAULT_CAPACITY = 16;
	/**
	 * The growth factor for reallocating the underlying array.  Java
	 * collections vary between 1.5 and 2.
	 */
	private static final double GROWTH_FACTOR = 2;
	/**
	 * The buffer holding elements in the channel.  Elements [head, tail) are
	 * in the channel; other elements are null.  Note that this buffer is
	 * managed circularly, and at least one element is empty before and after
	 * each method executes.
	 */
	private E[] buffer;
	/**
	 * The index of the first element in this channel (the element returned by
	 * pop() or peek(0)), or an arbitrary number equal to tail if the channel
	 * is empty.
	 */
	private int head;
	/**
	 * One greater than the index of the last element in this channel (that is,
	 * the index where an element would be added by push()), or an arbitrary
	 * number equal to head if the channel is empty.
	 */
	private int tail;
	/**
	 * The number of calls to push() since the last call to resetStatistics().
	 */
	private int pushCount;
	/**
	 * The number of calls to pop() since the last call to resetStatistics().
	 */
	private int popCount;
	/**
	 * The maximum index passed to peek() since the last call to
	 * resetStatistics(), relative to the position of head at the time of that
	 * last call to resetStatistics(). That is, a number useful for checking
	 * peek rate declarations.
	 */
	private int maxPeekIndex;

	/**
	 * Constructs an empty Channel with the default capacity.
	 */
	public Channel() {
		this(DEFAULT_CAPACITY);
	}

	/**
	 * Constructs an empty Channel with the given initial capacity.
	 * @param capacity the new Channel's initial capacity
	 */
	@SuppressWarnings("unchecked")
	public Channel(int capacity) {
		if (capacity < 0)
			throw new IllegalArgumentException();
		capacity = Math.min(capacity, DEFAULT_CAPACITY);
		buffer = (E[])new Object[capacity];
		resetStatistics();
	}

	/**
	 * Constructs a Channel containing the given elements.  The elements are
	 * copied into the channel; the given array is not modified.
	 * @param initialElements the initial elements
	 */
	public Channel(E[] initialElements) {
		this(Arrays.asList(initialElements));
	}

	/**
	 * Constructs a Channel containing the given elements.  The elements are
	 * copied into the channel; the given collection is not modified.
	 * @param initialElements the initial elements
	 */
	public Channel(Collection<? extends E> initialElements) {
		this(initialElements.size() + 1);
		for (E e : initialElements)
			buffer[tail++] = e;
	}

	/* Basic operations */

	/**
	 * Returns the element at the given index. Updates the maximum peek index.
	 * @param index the index to peek at
	 * @return the element at the given index
	 * @throws IndexOutOfBoundsException if index < 0 or index > size()
	 */
	public E peek(int index) {
		if (index < 0 || index > size())
			throw new IndexOutOfBoundsException(String.format("index %d, size %d", index, size()));
		//Compute the physical index.
		int physicalIndex = head + index;
		if (physicalIndex >= buffer.length)
			physicalIndex -= buffer.length;
		maxPeekIndex = Math.max(maxPeekIndex, index + popCount);
		return buffer[physicalIndex];
	}

	/**
	 * Adds the given element to the end of this channel. Updates the push
	 * count.
	 * @param element the element to add
	 */
	public void push(E element) {
		buffer[tail] = element;
		tail = increment(tail);
		if (head == tail)
			expandCapacity();
		++pushCount;
	}

	/**
	 * Removes and returns the element at the front of this channel.  Updates
	 * the pop count.
	 * @return the element at the front of this channel
	 * @throws NoSuchElementException if this channel is empty
	 */
	public E pop() {
		if (isEmpty())
			throw new NoSuchElementException("Channel is empty");
		E retval = buffer[head];
		//Help the garbage collector recognize this element as garbage.
		buffer[head] = null;
		head = increment(head);
		++popCount;
		return retval;
	}

	/**
	 * Returns this channel's logical size: the number of elements that can be
	 * popped.
	 * @return this channel's logical size
	 */
	public int size() {
		int size = tail - head;
		if (size < 0)
			size += buffer.length;
		return size;
	}

	/**
	 * Returns true iff this channel contains no elements.
	 * @return true iff this channel contains no elements.
	 */
	public boolean isEmpty()  {
		return head == tail;
	}

	/* Statistics-related operations */

	/**
	 * Returns the maximum index passed to peek() since the last call to
	 * resetStatistics(), relative to the front of this channel at the time of
	 * that last call.  If no peeking occurred, returns -1.
	 * @return the maximum peek index
	 */
	public int getMaxPeekIndex() {
		return maxPeekIndex;
	}

	/**
	 * Returns the number of calls to push() since the last call to
	 * resetStatistics().
	 * @return the push count
	 */
	public int getPushCount() {
		return pushCount;
	}

	/**
	 * Returns the number of calls to pop() since the last call to
	 * resetStatistics().
	 * @return the pop count
	 */
	public int getPopCount() {
		return popCount;
	}

	/**
	 * Resets this channel's statistics (push count, pop count and max peek
	 * index).
	 */
	public void resetStatistics() {
		popCount = 0;
		pushCount = 0;
		maxPeekIndex = -1;
	}

	/* Transitioning to other representations (compiler, debugging) */

	/**
	 * Returns the current capacity of this channel (the number of elements it
	 * can hold without triggering a reallocation).
	 * @return the channel's capacity
	 */
	public int getCapacity() {
		return buffer.length - 1;
	}

	/**
	 * Returns an iterator that iterates over the elements in this channel in
	 * first-to-last order.  The returned iterator does not support remove().
	 * @return an iterator over this channel
	 */
	@Override
	public Iterator<E> iterator() {
		return new Iterator<E>() {
			private int position = head;
			@Override
			public boolean hasNext() {
				return position != tail;
			}
			@Override
			public E next() {
				if (!hasNext())
					throw new NoSuchElementException();
				E retval = buffer[position];
				position = increment(position);
				return retval;
			}
			@Override
			public void remove() {
				//We could support remove() for a prefix of the elements
				//returned by next() (i.e., once you call next() twice in a row,
				//you can't remove() anymore).
				throw new UnsupportedOperationException("Not supported.");
			}
		};
	}

	@Override
	public String toString() {
		if (isEmpty())
			return "[]";
		Iterator<E> iterator = iterator();
		StringBuilder sb = new StringBuilder("[");
		sb.append(iterator.next());
		while (iterator.hasNext())
			sb.append(", ").append(iterator.next());
		return sb.toString();
	}

	/* Helper methods */

	/**
	 * Returns the index logically one greater than the given index, wrapping
	 * around the buffer size as required.
	 * @param index the index to increment
	 * @return the next index
	 */
	private int increment(int index) {
		++index;
		//TODO: we could trade the branch for a division by using % instead.
		if (index >= buffer.length)
			index -= buffer.length;
		return index;
	}

	/**
	 * Expand the buffer, copying over the existing elements.  We could take
	 * this opportunity to move all the elements to the front of the new buffer,
	 * but that would complicate the code merely to delay the next wraparound,
	 * so there's no real reason.
	 */
	private void expandCapacity() {
		int newLength = (int)Math.ceil(buffer.length * GROWTH_FACTOR);
		if (newLength <= 0)
			newLength = Integer.MAX_VALUE; //Good luck allocating that much...
		assert newLength >= buffer.length;
		buffer = Arrays.copyOf(buffer, newLength);
	}
}
