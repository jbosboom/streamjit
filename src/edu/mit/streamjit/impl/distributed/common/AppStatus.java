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
	}
}
