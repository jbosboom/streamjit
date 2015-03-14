/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.blob;

/**
 * A Buffer buffers data items on inter-Blob edges.
 * <p/>
 * Readers and/or writers may or may not block on reads and/or writes. Users
 * should be prepared to retry all operations in case of nonblocking failure. In
 * general, partial reads and writes may occur, but the readAll() methods
 * provide an atomicity guarantee (see their description for details). If the
 * Buffer is blocking and is interrupted, it clears the thread's interrupt
 * status and reports how much it did (possibly nothing) as though it was
 * nonblocking. (When draining, Blobs will need to check size() before reading
 * to avoid blocking forever; the caller of Blob.drain() will interrupt any
 * pending reads.)
 * <p/>
 * read() and read(T[], int, int) are provided for inter-machine I/O and for
 * CompiledStream. Blobs will probably want the atomicity guarantee of readAll()
 * so they don't have to perform any internal input buffering.
 * <p/>
 * An atomic writeAll() method is not provided because it would permit deadlock
 * with an atomic readAll() when there are not enough items to satisfy the read
 * but not enough space to satisfy the write.
 * <p/>
 * Buffer does not support buffer-flipping buffering strategies because they
 * would break encapsulation: Blobs would have to provide just the right size of
 * buffer to make swapping possible. As Buffers are used on inter-Blob edges,
 * buffer-flipping wouldn't be of much benefit: for edges due to dynamism,
 * flipping is often not possible because the buffers aren't entirely
 * full/empty; for inter-machine edges, flipping isn't possible over the
 * network. Thus flipping adds complexity for little benefit.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 7/17/2013
 */
public interface Buffer {
	/**
	 * Reads up to one data item from the buffer.
	 * @return the read item, or null if no data was read
	 */
	public Object read();

	/**
	 * Reads up to length data items from this buffer into the given array
	 * beginning at offset.
	 * @param data the array to write into
	 * @param offset the offset to begin writing at
	 * @param length the number of items to read
	 * @return the number of data items read (between 0 and length, inclusive)
	 */
	public int read(Object[] data, int offset, int length);

	/**
	 * Atomically reads enough data items from this buffer to fill the given
	 * array, or does nothing.
	 * @param data the array to write into
	 * @return true iff data was read
	 * @throws IllegalArgumentException if the array's length is greater than
	 * this buffer's capacity (so that the call would always "fail", causing an
	 * infinite retry loop)
	 */
	public boolean readAll(Object[] data);

	/**
	 * Atomically reads enough data items from this buffer to fill the given
	 * array starting from offset, or does nothing.
	 * @param data the array to read into
	 * @param offset the offset to begin writing at
	 * @return true iff data was read
	 * @throws IllegalArgumentException if the array's length is greater than
	 * this buffer's capacity (so that the call would always "fail", causing an
	 * infinite retry loop)
	 */
	public boolean readAll(Object[] data, int offset);

	/**
	 * Writes up to one data item into the buffer
	 * @param t the data item to write
	 * @return true iff data was written
	 */
	public boolean write(Object t);

	/**
	 * Writes up to length data items from the given array beginning at offset
	 * into this buffer.
	 * @param data the array to read from
	 * @param offset the offset to begin reading from
	 * @param length the number of data items to write
	 * @return the number of data items written (between 0 and length,
	 * inclusive)
	 */
	public int write(Object[] data, int offset, int length);

	/**
	 * Returns the number of data items currently in this buffer. Due to the
	 * potential for concurrent modification, the returned value is immediately
	 * stale. However, if there is only one reader, it can treat the return
	 * value as a lower bound, and if there is only one writer, it can treat the
	 * return value as an upper bound.
	 * @return this buffer's size
	 */
	public int size();

	/**
	 * Returns this buffer's capacity (the maximum number of data items this
	 * buffer can hold). The return value will not change over the life of the
	 * buffer. If a buffer expands as needed, it returns Integer.MAX_VALUE.
	 * @return this buffer's capacity
	 */
	public int capacity();
}
