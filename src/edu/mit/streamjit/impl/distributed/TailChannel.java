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
package edu.mit.streamjit.impl.distributed;

import edu.mit.streamjit.impl.distributed.common.BoundaryChannel.BoundaryInputChannel;

/**
 * This is a {@link BoundaryInputChannel} with counting facility.
 * Implementations need to count the number of elements received and provide
 * other services based on the count.
 * 
 * @author sumanan
 * @since 24 Jan, 2015
 */
public interface TailChannel extends BoundaryInputChannel {

	/**
	 * @return Number of elements received after the last reset()
	 */
	public int count();

	/**
	 * Returns the time to receive fixed number of outputs. The fixed number can
	 * be hard coded in side an implementation or passed as a constructor
	 * argument.
	 * <p>
	 * The caller will be blocked until the fixed number of outputs are
	 * received.
	 * 
	 * @return the time(ms) to receive fixed number of outputs.
	 * 
	 * @throws InterruptedException
	 */
	public long getFixedOutputTime() throws InterruptedException;

	/**
	 * Returns the time to receive fixed number of outputs. The fixed number can
	 * be hard coded in side an implementation or passed as a constructor
	 * argument. Waits at most <code>timeout</code> time to receive fixed number
	 * of output. Returns -1 if timeout occurred.
	 * 
	 * <p>
	 * If timeout < 1, then the behavior this method is equivalent to calling
	 * {@link #getFixedOutputTime()}.
	 * </p>
	 * 
	 * <p>
	 * The caller will be blocked until the fixed number of output is received
	 * or timeout occurred, whatever happens early.
	 * 
	 * @param timeout
	 *            Wait at most timeout time to receive fixed number of output.
	 * 
	 * @return the time(ms) to receive fixed number of outputs or -1 if timeout
	 *         occurred.
	 * 
	 * @throws InterruptedException
	 */
	public long getFixedOutputTime(long timeout) throws InterruptedException;

	/**
	 * Resets all counters and other resources. Any thread blocked on either
	 * {@link #getFixedOutputTime()} or {@link #getFixedOutputTime(long)} should
	 * be released after this call.
	 */
	public void reset();

}
