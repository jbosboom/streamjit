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
package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.impl.blob.Buffer;

/**
 * A ConcreteStorage that can write data items to an output Buffer in bulk (from
 * the ConcreteStorage's point of view, a bulk read operation).  Because Blobs
 * must perform short writes to avoid deadlock in multi-Blob graphs,
 * implementations and users must cope with short writes.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 12/5/2013
 */
public interface BulkReadableConcreteStorage extends ConcreteStorage {
	/**
	 * Copies {@code count} elements starting at {@code index} from this
	 * ConcreteStorage to {@code dest}.
	 * @param dest the buffer to copy to
	 * @param index the index of the first element to copy
	 * @param count the number of element to copy
	 * @return the number of items copied
	 */
	public int bulkRead(Buffer dest, int index, int count);
}
