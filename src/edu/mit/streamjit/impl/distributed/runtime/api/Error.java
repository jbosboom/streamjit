package edu.mit.streamjit.impl.distributed.runtime.api;

/**
 * To avoid unnecessary communication overhead and keep the communication between the StreamJit nodes optimal, don't implement
 * unnecessary functions here. Instead implement enum_dispatcher to dispatch the enum. Just for the testing purpose I implemented
 * toString().
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 17, 2013
 */
public enum Error implements MessageElement {

	FILE_NOT_FOUND {
		@Override
		public void process(ErrorProcessor errorProcessor) {
			errorProcessor.processFILE_NOT_FOUND();
		}
	}, WORKER_NOT_FOUND {
		@Override
		public void process(ErrorProcessor errorProcessor) {
			errorProcessor.processWORKER_NOT_FOUND();
		}
	};

	@Override
	public void accept(MessageVisitor visitor) {
		visitor.visit(this);
	}

	
	public abstract void process(ErrorProcessor errorProcessor);
	
	/*
	 * public String toString() { String msg; switch (this) { case JAR_FILE_NOT_FOUND: msg =
	 * "Error: Jar file of the application not found"; break; case TOP_LEVEL_WORKER_NOT_FOUND: msg =
	 * "Error: Top level filter not found in the jar file"; break; default: msg = "toString for this enum type need to be implemented";
	 * break; } return msg; }
	 */
};
