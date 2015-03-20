package edu.mit.streamjit.impl.distributed.profiler;

import edu.mit.streamjit.impl.distributed.common.CTRLRMessageElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageVisitor;
import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * ProfilerCommand can be send by a {@link Controller} to {@link StreamNode} to
 * carry profiling related actions.
 * 
 * @author sumanan
 * @since 27 Jan, 2015
 */
public enum ProfilerCommand implements CTRLRMessageElement {
	/**
	 * Starts the profiler.
	 */
	START {
		@Override
		public void process(ProfilerCommandProcessor commandProcessor) {
			commandProcessor.processSTART();
		}
	},
	/**
	 * Stops the profiler.
	 */
	STOP {
		@Override
		public void process(ProfilerCommandProcessor commandProcessor) {
			commandProcessor.processSTOP();
		}
	},

	/**
	 * Pause the profiler
	 */
	PAUSE {
		@Override
		public void process(ProfilerCommandProcessor commandProcessor) {
			commandProcessor.processPAUSE();
		}

	},
	/**
	 * Resume the profiler
	 */
	RESUME {
		@Override
		public void process(ProfilerCommandProcessor commandProcessor) {
			commandProcessor.processRESUME();
		}
	};

	@Override
	public void accept(CTRLRMessageVisitor visitor) {
		visitor.visit(this);
	}

	public abstract void process(ProfilerCommandProcessor commandProcessor);

	public interface ProfilerCommandProcessor {

		public void processSTART();

		public void processSTOP();

		public void processPAUSE();

		public void processRESUME();

	}
}
