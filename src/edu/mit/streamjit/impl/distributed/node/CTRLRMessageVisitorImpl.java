package edu.mit.streamjit.impl.distributed.node;

import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageVisitor;
import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements;
import edu.mit.streamjit.impl.distributed.common.MiscCtrlElements.MiscCtrlElementProcessor;
import edu.mit.streamjit.impl.distributed.common.Request;
import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement.CTRLRDrainProcessor;
import edu.mit.streamjit.impl.distributed.common.Command.CommandProcessor;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString.ConfigurationStringProcessor;
import edu.mit.streamjit.impl.distributed.common.Request.RequestProcessor;

/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
public class CTRLRMessageVisitorImpl implements CTRLRMessageVisitor {

	private final CommandProcessor cp;
	private final RequestProcessor rp;
	private final ConfigurationStringProcessor jp;
	private final CTRLRDrainProcessor dp;
	private final MiscCtrlElementProcessor miscProcessor;

	public CTRLRMessageVisitorImpl(CommandProcessor cp, RequestProcessor rp,
			ConfigurationStringProcessor jp, CTRLRDrainProcessor dp,
			MiscCtrlElementProcessor miscProcessor) {
		this.cp = cp;
		this.rp = rp;
		this.jp = jp;
		this.dp = dp;
		this.miscProcessor = miscProcessor;
	}

	@Override
	public void visit(Command streamJitCommand) {
		streamJitCommand.process(cp);
	}

	@Override
	public void visit(Request request) {
		request.process(rp);
	}

	@Override
	public void visit(ConfigurationString json) {
		json.process(jp);
	}

	@Override
	public void visit(CTRLRDrainElement ctrlrDrainElement) {
		ctrlrDrainElement.process(dp);
	}

	@Override
	public void visit(MiscCtrlElements miscCtrlElements) {
		miscCtrlElements.process(miscProcessor);
	}
}
