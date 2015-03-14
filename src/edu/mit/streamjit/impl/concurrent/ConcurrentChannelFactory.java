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
package edu.mit.streamjit.impl.concurrent;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.interp.ArrayChannel;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.impl.interp.ChannelFactory;

/**
 * This {@link ChannelFactory} manufactures {@link ArrayChannel}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 28, 2013
 */
public class ConcurrentChannelFactory implements ChannelFactory {
	@Override
	public <E> Channel<E> makeChannel(Worker<?, E> upstream,
			Worker<E, ?> downstream) {
		return new ArrayChannel<E>();
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof ConcurrentChannelFactory;
	}

	@Override
	public int hashCode() {
		return 0;
	}
}