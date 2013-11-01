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

	private final ErrorProcessor ep;
	private final SystemInfoProcessor sip;
	private final AppStatusProcessor ap;
	private final NodeInfoProcessor np;
	private final SNExceptionProcessor snExP;
	private StreamJitAppManager manager = null;

	public SNMessageVisitorImpl(ErrorProcessor ep, SystemInfoProcessor sip,
			AppStatusProcessor ap, NodeInfoProcessor np,
			SNExceptionProcessor snExP) {
		this.ep = ep;
		this.sip = sip;
		this.ap = ap;
		this.np = np;
		this.snExP = snExP;
	}

	@Override
	public void visit(Error error) {
		error.process(ep);
	}

	@Override
	public void visit(SystemInfo systemInfo) {
		sip.process(systemInfo);
	}

	@Override
	public void visit(AppStatus appStatus) {
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
		snExP.process(snException);
	}

	public void registerManager(StreamJitAppManager manager) {
		assert manager == null : "StreamJitAppManager has already been set";
		this.manager = manager;
	}
}
