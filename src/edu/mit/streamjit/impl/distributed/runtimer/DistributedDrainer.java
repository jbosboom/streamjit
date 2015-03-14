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
package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.AbstractDrainer;
import edu.mit.streamjit.impl.distributed.StreamJitAppManager;

/**
 * @author Sumanan sumanan@mit.edu
 * @since Aug 13, 2013
 */
public class DistributedDrainer extends AbstractDrainer {

	StreamJitAppManager manager;

	public DistributedDrainer(StreamJitAppManager manager) {
		this.manager = manager;
		// Read this. Don't let the "this" reference escape during construction
		// http://www.ibm.com/developerworks/java/library/j-jtp0618/
		manager.setDrainer(this);
	}

	@Override
	protected void drainingDone(boolean isFinal) {
		manager.drainingFinished(isFinal);
	}

	@Override
	protected void drain(Token blobID, boolean isFinal) {
		manager.drain(blobID, isFinal);
	}

	@Override
	protected void drainingDone(Token blobID, boolean isFinal) {
		// Nothing to clean in Distributed case.
	}

	@Override
	protected void prepareDraining(boolean isFinal) {
		manager.drainingStarted(isFinal);
	}
}
