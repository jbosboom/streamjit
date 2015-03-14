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
 * {@link StreamNode}s may send the status of the stream application to the
 * {@link Controller}. Controller may request a stream node to send the app
 * status by sending the request message {@link Request}.APPStatus.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 17, 2013
 */
public enum AppStatus implements SNMessageElement {
	/**
	 * Stream application is still running.
	 */
	RUNNING {
		@Override
		public void process(AppStatusProcessor apstatusProcessor) {
			apstatusProcessor.processRUNNING();
		}
	},
	/**
	 * Stream application has been stopped.
	 */
	STOPPED {
		@Override
		public void process(AppStatusProcessor apstatusProcessor) {
			apstatusProcessor.processSTOPPED();
		}
	},
	/**
	 * Error when executing the stream application.
	 */
	ERROR {
		@Override
		public void process(AppStatusProcessor apstatusProcessor) {
			apstatusProcessor.processERROR();
		}
	},
	/**
	 * Stream application is ready to execute but not started yet. Controller
	 * may issue start command to begin the execution.
	 */
	NOT_STARTED {
		@Override
		public void process(AppStatusProcessor apstatusProcessor) {
			apstatusProcessor.processNOT_STARTED();
		}
	},
	/**
	 * No any stream application is submitted for execution. Stream node does
	 * nothing.
	 */
	NO_APP {
		@Override
		public void process(AppStatusProcessor apstatusProcessor) {
			apstatusProcessor.processNO_APP();
		}
	},
	/**
	 * Blobs are compiled. Ready for execution.
	 */
	COMPILED {
		@Override
		public void process(AppStatusProcessor apstatusProcessor) {
			apstatusProcessor.processCOMPILED();
		}
	},
	/**
	 * Compile time error/exception. Mainly due to illegal configuration.
	 */
	COMPILATION_ERROR {
		@Override
		public void process(AppStatusProcessor apstatusProcessor) {
			apstatusProcessor.processCOMPILATION_ERROR();
		}
	};

	@Override
	public void accept(SNMessageVisitor visitor) {
		visitor.visit(this);
	}

	public abstract void process(AppStatusProcessor apstatusProcessor);

	/**
	 * {@link StreamNode}s and {@link Controller} should implement this
	 * interfaces in order to correctly process the {@link AppStatus}. It has
	 * interface function to each enum in the app status.
	 * 
	 * @author Sumanan
	 */
	public interface AppStatusProcessor {

		public void processRUNNING();

		public void processSTOPPED();

		public void processERROR();

		public void processNOT_STARTED();

		public void processNO_APP();

		public void processCOMPILED();

		public void processCOMPILATION_ERROR();
	}
}
