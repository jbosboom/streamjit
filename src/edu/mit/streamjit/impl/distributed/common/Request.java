package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.distributed.node.StreamNode;
import edu.mit.streamjit.impl.distributed.runtimer.Controller;

/**
 * {@link Controller} may request any of following information from a
 * {@link StreamNode}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
public enum Request implements CTRLRMessageElement {
	/**
	 * Status of the streaming application. Once this is received,
	 * {@link StreamNode} must send it's app status to the controller. See
	 * {@link AppStatus} for further information.
	 */
	APPStatus {
		@Override
		public void process(RequestProcessor reqProcessor) {
			reqProcessor.processAPPStatus();
		}
	},
	/**
	 * Current hardware and low level information of the system. Once this is
	 * received, {@link StreamNode} must sent it's {@link SystemInfo} to the
	 * {@link Controller}.
	 */
	SysInfo {
		@Override
		public void process(RequestProcessor reqProcessor) {
			reqProcessor.processSysInfo();
		}
	},
	/**
	 * Assigned machine id.
	 */
	machineID {
		@Override
		public void process(RequestProcessor reqProcessor) {
			reqProcessor.processMachineID();
		}
	},
	/**
	 * Node information of the system that the {@link StreamNode} is running.
	 * Once it is received, Stream node should send the {@link NodeInfo} to the
	 * {@link Controller}.
	 */
	NodeInfo {
		@Override
		public void process(RequestProcessor reqProcessor) {
			reqProcessor.processNodeInfo();
		}
	},
	/**
	 * This command is to ask StreamNode to exit.
	 */
	EXIT {
		@Override
		public void process(RequestProcessor reqProcessor) {
			reqProcessor.processEXIT();
		}
	};

	@Override
	public void accept(CTRLRMessageVisitor visitor) {
		visitor.visit(this);
	}

	public abstract void process(RequestProcessor reqProcessor);

	/**
	 * {@link StreamNode}s and {@link Controller} should implement this
	 * interfaces in order to correctly process the {@link Request}. It has
	 * interface function for each enum in the request. Based on the received
	 * enum, appropriate function will be called.
	 */
	public interface RequestProcessor {

		public void processAPPStatus();

		public void processSysInfo();

		public void processMachineID();

		public void processNodeInfo();

		public void processEXIT();
	}
}
