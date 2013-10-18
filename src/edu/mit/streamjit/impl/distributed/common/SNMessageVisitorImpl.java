package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.distributed.common.AppStatus.AppStatusProcessor;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.CTRLRDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationStringProcessor;
import edu.mit.streamjit.impl.distributed.common.Error.ErrorProcessor;
import edu.mit.streamjit.impl.distributed.common.NodeInfo.NodeInfoProcessor;
import edu.mit.streamjit.impl.distributed.common.Request.RequestProcessor;
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
	private final SNDrainProcessor dp;

	public SNMessageVisitorImpl(ErrorProcessor ep, SystemInfoProcessor sip,
			AppStatusProcessor ap, NodeInfoProcessor np, SNDrainProcessor dp) {
		this.ep = ep;
		this.sip = sip;
		this.ap = ap;
		this.np = np;
		this.dp = dp;
	}

	@Override
	public void visit(Error error) {
		error.process(ep);
	}

	@Override
	public void visit(SystemInfo systemInfo) {
		// systemInfo.
	}

	@Override
	public void visit(AppStatus appStatus) {
		appStatus.process(ap);
	}

	@Override
	public void visit(NodeInfo nodeInfo) {
	}
	@Override
	public void visit(SNDrainElement snDrainElement) {
		snDrainElement.process(dp);
	}
}
