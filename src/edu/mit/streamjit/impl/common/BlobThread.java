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
package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.impl.blob.Blob;

/**
 * Runner thread to run a core code of a {@link Blob}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since Jul 25, 2013
 */
public final class BlobThread extends Thread {
	private volatile boolean stopping = false;
	private final Runnable coreCode;

	public BlobThread(Runnable coreCode, String name) {
		super(name);
		this.coreCode = coreCode;
	}

	public BlobThread(Runnable coreCode) {
		this.coreCode = coreCode;
	}

	@Override
	public void run() {
		while (!stopping)
			coreCode.run();
	}

	public void requestStop() {
		stopping = true;
	}
}