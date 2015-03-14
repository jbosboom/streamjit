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
package edu.mit.streamjit.util;

/**
 * Throws checked exceptions as if unchecked.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/13/2014
 */
public final class SneakyThrows {
	private SneakyThrows() {}

	/**
	 * Throws the given Throwable, even if it's a checked exception the caller
	 * could not otherwise throw.
	 *
	 * This method returns RuntimeException to enable "throw sneakyThrow(t);"
	 * syntax to convince Java's dataflow analyzer that an exception will be
	 * thrown.
	 *
	 * Note that catching sneakythrown exceptions can be difficult as Java will
	 * complain about attempts to catch checked exceptions that "cannot" be
	 * thrown from the try-block body.
	 * @param t the Throwable to throw
	 * @return never returns
	 */
	@SuppressWarnings("deprecation")
	public static RuntimeException sneakyThrow(Throwable t) {
		Thread.currentThread().stop(t);
		throw new AssertionError();
	}
}
