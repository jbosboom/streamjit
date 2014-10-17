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
package edu.mit.streamjit.api;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The interface to a compiled stream.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 11/20/2012
 */
public interface CompiledStream {
	/**
	 * Returns true if the stream has finished draining. Once this method
	 * returns true, it will always return true in the future. Once a thread
	 * observes this method to return true, all data has been written to the
	 * output (if using ManualOutput, the thread can retrieve all output data
	 * items by calling ManualOutput.poll() until poll() returns null).
	 * <p/>
	 * Note that busy-waiting on isDrained() without calling poll() may lead to
	 * deadlock; either alternate polling and checking for draining, or use two
	 * separate threads.
	 * @return true if the stream has finished draining
	 */
	public boolean isDrained();

	public void awaitDrained() throws InterruptedException;
	public void awaitDrained(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;
}
