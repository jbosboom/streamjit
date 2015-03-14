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
package edu.mit.streamjit.impl.distributed.common;

import java.io.IOException;
import java.net.Socket;

/**
 * This is a thread safe {@link TCPConnection}. Write and read operations are
 * thread safe and both are synchronized separately.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Oct 16, 2013
 */
public class SynchronizedTCPConnection extends TCPConnection {

	private final Object writeLock = new Object();
	private final Object readLock = new Object();

	/**
	 * TODO: Need to expose resetCount outside.
	 * 
	 * @param socket
	 */
	public SynchronizedTCPConnection(Socket socket) {
		super(socket, 50);
	}

	@Override
	public <T> T readObject() throws IOException, ClassNotFoundException {
		synchronized (readLock) {
			return super.readObject();
		}
	}

	@Override
	public void writeObject(Object obj) throws IOException {
		synchronized (writeLock) {
			super.writeObject(obj);
		}
	}
}
