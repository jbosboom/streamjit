/**
 * @author Sumanan sumanan@mit.edu
 * @since May 17, 2013
 */
package edu.mit.streamjit.impl.distributed.api;

public enum Command implements MessageElement {
	/**
	 * Starts the StreamJit Application.
	 */
	START {
		@Override
		public void process(CommandProcessor commandProcessor) {
			commandProcessor.processSTART();
		}
	},
	/**
	 * Stops the StreamJit Application. Not the StreamNode. Blobs must be drained before stopping.
	 */
	STOP {
		@Override
		public void process(CommandProcessor commandProcessor) {
			commandProcessor.processSTOP();
		}
	},
	SUSPEND {
		@Override
		public void process(CommandProcessor commandProcessor) {
			commandProcessor.processSUSPEND();
		}
	},
	RESUME {
		@Override
		public void process(CommandProcessor commandProcessor) {
			commandProcessor.processRESUME();
		}
	},
	/**
	 * This command is to ask StreamNode to exit.
	 */
	EXIT {
		@Override
		public void process(CommandProcessor commandProcessor) {
			commandProcessor.processEXIT();
		}
	};

	@Override
	public void accept(MessageVisitor visitor) {
		visitor.visit(this);
	}

	public abstract void process(CommandProcessor commandProcessor);

}
