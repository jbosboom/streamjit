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
	},
	BLOB_NOT_FOUND {
		@Override
		public void process(ErrorProcessor errorProcessor) {
			errorProcessor.processBLOB_NOT_FOUND();
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
		
		public void processBLOB_NOT_FOUND();

	}
};
