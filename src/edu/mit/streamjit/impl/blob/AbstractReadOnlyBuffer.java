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
 * A Buffer implementation whose write methods throw
 * UnsupportedOperationException.  Note that a read-only buffer is not immutable
 * because reading consumes elements from it, changes its size(), etc.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/21/2013
 */
public abstract class AbstractReadOnlyBuffer extends AbstractBuffer {
	@Override
	public boolean write(Object t) {
		throw new UnsupportedOperationException("read-only buffer");
	}
	@Override
	public int write(Object[] data, int offset, int length) {
		throw new UnsupportedOperationException("read-only buffer");
	}
	@Override
	public int capacity() {
		//The only reason to check a buffer's capacity is to see if there's room
		//to write into it, but we're read-only, so it shouldn't matter.  But
		//we need to return something constant and at least as big as size.
		return Integer.MAX_VALUE;
	}
}
