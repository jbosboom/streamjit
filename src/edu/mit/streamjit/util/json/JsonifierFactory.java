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
package edu.mit.streamjit.util.json;

/**
 * A JsonifierFactory creates Jsonifier instances for a particular type.
 *
 * A JsonifierFactory need not create a new instance for each request, and it
 * may return the same instance to several different requests if that instance
 * can handle all the types.
 *
 * Instances of this class should be thread-safe and reentrant; that is, methods
 * on this class may be called simultaneously by any number of threads,
 * including multiple times from a single thread (recursively).
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/25/2013
 */
public interface JsonifierFactory {
	/**
	 * Gets a Jsonifier instance for the given type, or null if this factory
	 * does not support the given type.  This is typically used when
	 * serializing.
	 * @param <T> the type
	 * @param klass the type
	 * @return a Jsonifier instance for the type, or null
	 */
	public <T> Jsonifier<T> getJsonifier(Class<T> klass);
}
