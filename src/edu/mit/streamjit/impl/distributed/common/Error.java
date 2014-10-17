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
 * Error may be send by a {@link StreamNode} to the {@link Controller}.
 * Controller must respond the error messages.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 17, 2013
 */
public enum Error implements SNMessageElement {

	FILE_NOT_FOUND {
		@Override
		public void process(ErrorProcessor errorProcessor) {
			errorProcessor.processFILE_NOT_FOUND();
		}
	},
	WORKER_NOT_FOUND {
		@Override
		public void process(ErrorProcessor errorProcessor) {
			errorProcessor.processWORKER_NOT_FOUND();
		}
	};

	@Override
	public void accept(SNMessageVisitor visitor) {
		visitor.visit(this);
	}

	public abstract void process(ErrorProcessor errorProcessor);

	// public String toString() {
	// String msg;
	// switch (this) {
	// case JAR_FILE_NOT_FOUND:
	// msg = "Error: Jar file of the application not found";
	// break;
	// case TOP_LEVEL_WORKER_NOT_FOUND:
	// msg = "Error: Top level filter not found in the jar file";
	// break;
	// default:
	// msg = "toString for this enum type need to be implemented";
	// break;
	// }
	// return msg;
	// }

	/**
	 * {@link StreamNode}s and {@link Controller} should implement this
	 * interface in order to correctly process the {@link Error}. It has
	 * interface function to each enum in the error. Based on the received enum,
	 * appropriate function will be called.
	 */
	public interface ErrorProcessor {

		public void processFILE_NOT_FOUND();

		public void processWORKER_NOT_FOUND();

	}
};
