package edu.mit.streamjit.impl.distributed.runtimer;

import edu.mit.streamjit.impl.distributed.StreamJitAppManager;
import edu.mit.streamjit.impl.distributed.common.AppStatus;
import edu.mit.streamjit.impl.distributed.common.Error;
import edu.mit.streamjit.impl.distributed.common.NodeInfo;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement;
import edu.mit.streamjit.impl.distributed.common.SNException;
import edu.mit.streamjit.impl.distributed.common.SNException.SNExceptionProcessor;
import edu.mit.streamjit.impl.distributed.common.SNMessageVisitor;
import edu.mit.streamjit.impl.distributed.common.SystemInfo;
import edu.mit.streamjit.impl.distributed.common.AppStatus.AppStatusProcessor;
import edu.mit.streamjit.impl.distributed.common.Error.ErrorProcessor;
import edu.mit.streamjit.impl.distributed.common.NodeInfo.NodeInfoProcessor;
import edu.mit.streamjit.impl.distributed.common.SNDrainElement.SNDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.SystemInfo.SystemInfoProcessor;

/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
public class SNMessageVisitorImpl implements SNMessageVisitor {

	private final SystemInfoProcessor sip;
	private final NodeInfoProcessor np;
	private StreamJitAppManager manager = null;

	public SNMessageVisitorImpl(SystemInfoProcessor sip, NodeInfoProcessor np) {
		this.sip = sip;
		this.np = np;
	}

	@Override
	public void visit(Error error) {
		assert manager != null : "StreamJitAppManager has not been set";
		ErrorProcessor ep = manager.errorProcessor();
		error.process(ep);
	}

	@Override
	public void visit(SystemInfo systemInfo) {
		sip.process(systemInfo);
	}

	@Override
	public void visit(AppStatus appStatus) {
		assert manager != null : "StreamJitAppManager has not been set";
		AppStatusProcessor ap = manager.appStatusProcessor();
		if (ap == null) {
			System.err.println("No AppStatusProcessor processor.");
			return;
		}
		appStatus.process(ap);
	}

	@Override
	public void visit(NodeInfo nodeInfo) {
		np.process(nodeInfo);
	}

	@Override
	public void visit(SNDrainElement snDrainElement) {
		assert manager != null : "StreamJitAppManager has not been set";
		SNDrainProcessor dp = manager.drainProcessor();
		if (dp == null) {
			System.err.println("No drainer processor.");
			return;
		}
		snDrainElement.process(dp);
	}

	@Override
	public void visit(SNException snException) {
		assert manager != null : "StreamJitAppManager has not been set";
		SNExceptionProcessor snExP = manager.exceptionProcessor();
		snException.process(snExP);
	}

	public void registerManager(StreamJitAppManager manager) {
		assert manager == null : "StreamJitAppManager has already been set";
		this.manager = manager;
	}
}
