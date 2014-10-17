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

import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * A command can be send by a {@link Controller} to {@link StreamNode} to carry
 * action on the stream blobs.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 17, 2013
 */
public enum Command implements CTRLRMessageElement {
	/**
	 * Starts the StreamJit Application. Once all blobs are set, Stream nodes
	 * will wait for start command from the controller to start the execution.
	 */
	START {
		@Override
		public void process(CommandProcessor commandProcessor) {
			commandProcessor.processSTART();
		}
	},
	/**
	 * Stops the StreamJit Application. Not the StreamNode. {@link Controller}
	 * can issue this command to stop the execution of the stream application in
	 * exceptional situation such as some other stream node is failed. Execution
	 * of application will be stopped. No draining carried.
	 */
	STOP {
		@Override
		public void process(CommandProcessor commandProcessor) {
			commandProcessor.processSTOP();
		}
	};

	@Override
	public void accept(CTRLRMessageVisitor visitor) {
		visitor.visit(this);
	}

	public abstract void process(CommandProcessor commandProcessor);

	/**
	 * Processes the {@link Command}s received from {@link Controller}. Based on
	 * the received command, appropriate function of this interface will be
	 * called.
	 * 
	 * @author Sumanan sumanan@mit.edu
	 * @since May 20, 2013
	 */
	public interface CommandProcessor {

		public void processSTART();

		public void processSTOP();

	}
}
