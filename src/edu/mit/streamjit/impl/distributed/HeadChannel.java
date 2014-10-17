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

import com.google.common.collect.ImmutableList;

import edu.mit.streamjit.impl.blob.AbstractReadOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionProvider;
import edu.mit.streamjit.impl.distributed.node.TCPOutputChannel;

/**
 * Head Channel is just a wrapper to TCPOutputChannel that skips
 * fillUnprocessedData.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Oct 21, 2013
 */
public class HeadChannel extends TCPOutputChannel {

	public HeadChannel(Buffer buffer, TCPConnectionProvider conProvider,
			TCPConnectionInfo conInfo, String bufferTokenName, int debugPrint) {
		super(buffer, conProvider, conInfo, bufferTokenName, debugPrint);
	}

	protected void fillUnprocessedData() {
		this.unProcessedData = ImmutableList.of();
	}

	/**
	 * Head HeadBuffer is just a wrapper to to a buffer that triggers final
	 * draining process if it finds out that there is no more data in the
	 * buffer. This is need for non manual inputs.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since Oct 2, 2013
	 */
	public static class HeadBuffer extends AbstractReadOnlyBuffer {

		Buffer buffer;
		AbstractDrainer drainer;

		public HeadBuffer(Buffer buffer, AbstractDrainer drainer) {
			this.buffer = buffer;
			this.drainer = drainer;
		}

		// TODO: Need to optimise the buffer reading. I will come back here
		// later.
		@Override
		public Object read() {
			Object o = buffer.read();
			if (buffer.size() == 0) {
				new DrainerThread().start();
			}
			return o;
		}

		@Override
		public int size() {
			return buffer.size();
		}

		class DrainerThread extends Thread {
			DrainerThread() {
				super("DrainerThread");
			}

			public void run() {
				System.out.println("Input data finished");
				drainer.startDraining(2);
			}
		}
	}
}