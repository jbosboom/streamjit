/**
 * @author Sumanan sumanan@mit.edu
 * @since May 17, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.api;

public enum AppStatus implements MessageElement {
	RUNNING {
		@Override
		public void process(AppStatusProcessor apstatusProcessor) {
			apstatusProcessor.processRUNNING();
		}
	},
	STOPPED {
		@Override
		public void process(AppStatusProcessor apstatusProcessor) {
			apstatusProcessor.processSTOPPED();
		}
	},
	ERROR {
		@Override
		public void process(AppStatusProcessor apstatusProcessor) {
			apstatusProcessor.processERROR();
		}
	},
	NOT_STARTED {
		@Override
		public void process(AppStatusProcessor apstatusProcessor) {
			apstatusProcessor.processNOT_STARTED();
		}
	},
	WAITING {
		@Override
		public void process(AppStatusProcessor apstatusProcessor) {
			apstatusProcessor.processWAITING();
		}
	};

	@Override
	public void accept(MessageVisitor visitor) {
		visitor.visit(this);
	}

	public abstract void process(AppStatusProcessor apstatusProcessor);
}
