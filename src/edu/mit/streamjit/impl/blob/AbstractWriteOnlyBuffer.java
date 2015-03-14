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
 * A Buffer implementation whose read methods throw
 * UnsupportedOperationException.
 *
 * This implementation overrides size() to return 0 and capacity() to return
 * Integer.MAX_VALUE, which is appropriate for Buffers for overall output, where
 * write-only buffers are most useful.  Implementations with actual capacity
 * constraints, such as a Buffer that writes into a specific array, should
 * probably throw an exception if that capacity is exceeded, rather than
 * advertising a fixed capacity, as the latter may lead to infinite loops or
 * blocking as the stream waits for space.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/21/2013
 */
public abstract class AbstractWriteOnlyBuffer extends AbstractBuffer {
	@Override
	public Object read() {
		throw new UnsupportedOperationException("write-only buffer");
	}
	@Override
	public int read(Object[] data, int offset, int length) {
		throw new UnsupportedOperationException("write-only buffer");
	}
	@Override
	public boolean readAll(Object[] data) {
		throw new UnsupportedOperationException("write-only buffer");
	}
	@Override
	public boolean readAll(Object[] data, int offset) {
		throw new UnsupportedOperationException("write-only buffer");
	}
	@Override
	public int size() {
		return 0;
	}
	@Override
	public int capacity() {
		return Integer.MAX_VALUE;
	}
}
