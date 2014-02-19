package edu.mit.streamjit.impl.blob;

/**
 * A Buffer supporting nondestructive indexed reads (peeks).  In addition to the read
 * methods defined in Buffer, clients of PeekableBuffer may peek at items at
 * indices less than the buffer's size(), then consume them with the
 * consume(int) method.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 2/16/2014
 */
public interface PeekableBuffer extends Buffer {
	/**
	 * Peeks at the element at the given index (relative to the front of this
	 * buffer; {@code peek(0)} returns the element that would be returned by
	 * {@link #read()}).
	 * @param index the index to peek at
	 * @return the element at the given index
	 * @throws IndexOutOfBoundsException if {@code index >=} {@link size() size()}
	 */
	public Object peek(int index);

	/**
	 * Consumes the given number of items from this buffer, as if by repeated
	 * calls to {@link #read()}.
	 * @param items the number of items to consume
	 * @throws IndexOutOfBoundsException if {@code index >=} {@link size() size()}
	 */
	public void consume(int items);
}
