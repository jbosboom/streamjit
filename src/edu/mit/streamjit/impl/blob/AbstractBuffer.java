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
 * AbstractBuffer implements some Buffer methods in terms of read() and write().
 * Custom implementations, when supported by the underlying storage, will
 * generally yield better performance.
 * <p/>
 * Note that this class (and any subclass inheriting its methods) assumes at
 * most one reader and at most one writer are using the buffer at once. If more
 * readers or writers use an instance at once, the bulk reads and writes may not
 * be atomic. If reading might fail due to interruption, readAll(Object[], int)
 * must be overridden to ignore interrupts.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 7/27/2013
 */
public abstract class AbstractBuffer implements Buffer {
	@Override
	public int read(Object[] data, int offset, int length) {
		int read = 0;
		Object obj;
		while (read < length && (obj = read()) != null) {
			data[offset++] = obj;
			++read;
		}
		return read;
	}

	@Override
	public boolean readAll(Object[] data) {
		return readAll(data, 0);
	}

	@Override
	public boolean readAll(Object[] data, int offset) {
		int required = data.length - offset;
		if (required > size())
			return false;
		for (; offset < data.length; ++offset) {
			Object e = read();
			//We checked size() above, so we should never fail here, except in
			//case of concurrent modification by another reader.
			assert e != null;
			data[offset] = e;
		}
		return true;
	}

	@Override
	public int write(Object[] data, int offset, int length) {
		int written = 0;
		while (written < length && write(data[offset++]))
			++written;
		return written;
	}
}
