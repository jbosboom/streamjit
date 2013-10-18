package edu.mit.streamjit.impl.distributed.node;

import edu.mit.streamjit.impl.distributed.common.CTRLRDrainElement;
import edu.mit.streamjit.impl.distributed.common.CTRLRMessageVisitor;
import edu.mit.streamjit.impl.distributed.common.Command;
import edu.mit.streamjit.impl.distributed.common.ConfigurationString;
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

	public CTRLRMessageVisitorImpl(CommandProcessor cp, RequestProcessor rp,
			ConfigurationStringProcessor jp, CTRLRDrainProcessor dp) {
		this.cp = cp;
		this.rp = rp;
		this.jp = jp;
		this.dp = dp;
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
}
