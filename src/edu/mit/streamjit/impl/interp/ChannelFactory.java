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
package edu.mit.streamjit.impl.interp;

import edu.mit.streamjit.api.Worker;

/**
 * A Channel factory.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/21/2013
 */
public interface ChannelFactory {
	/**
	 * Creates a channel that will be used to connect the two given workers. If
	 * an implementation doesn't care what channel is used to connect the two
	 * filters, consider returning EmptyChannel. Implementations need not create
	 * a new channel for each call, though not doing so may produce strange
	 * results. Implementations may return null; the results may vary from not
	 * connecting the two workers with a channel to throwing an exception.
	 * <p/>
	 * TODO: Generic bounds are too strict -- the filters don't have to exactly
	 * agree on type, but merely be compatible. But note that isn't of much
	 * import due to erasure.
	 * @param <E> the type of element in the channel
	 * @param upstream the upstream worker (adds elements to the channel), or
	 * null for the overall stream graph input
	 * @param downstream the downstream worker (removes elements from the
	 * channel), or null for the overall stream graph output
	 * @return a channel to connect the two workers
	 */
	public <E> Channel<E> makeChannel(Worker<?, E> upstream, Worker<E, ?> downstream);
}
