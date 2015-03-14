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
 * A Buffer supporting nondestructive indexed reads (peeks).  In addition to the read
 * methods defined in Buffer, clients of PeekableBuffer may peek at items at
 * indices less than the buffer's size(), then consume them with the
 * consume(int) method.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
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
