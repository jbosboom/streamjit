/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.node;

import edu.mit.streamjit.impl.distributed.api.AppStatus;
import edu.mit.streamjit.impl.distributed.api.AppStatusProcessor;
import edu.mit.streamjit.impl.distributed.api.Command;
import edu.mit.streamjit.impl.distributed.api.CommandProcessor;
import edu.mit.streamjit.impl.distributed.api.Error;
import edu.mit.streamjit.impl.distributed.api.ErrorProcessor;
import edu.mit.streamjit.impl.distributed.api.JsonString;
import edu.mit.streamjit.impl.distributed.api.JsonStringProcessor;
import edu.mit.streamjit.impl.distributed.api.MessageVisitor;
import edu.mit.streamjit.impl.distributed.api.Request;
import edu.mit.streamjit.impl.distributed.api.RequestProcessor;
import edu.mit.streamjit.impl.distributed.api.SystemInfo;

public class NodeMessageVisitor implements MessageVisitor {

	private AppStatusProcessor asp;
	private CommandProcessor cp;
	private ErrorProcessor ep;
	private RequestProcessor rp;
	private JsonStringProcessor jp;

	public NodeMessageVisitor(AppStatusProcessor asp, CommandProcessor cp, ErrorProcessor ep, RequestProcessor rp, JsonStringProcessor jp) {
		this.asp = asp;
		this.cp = cp;
		this.ep = ep;
		this.rp = rp;
		this.jp = jp;
	}

	@Override
	public void visit(AppStatus appStatus) {
		appStatus.process(asp);
	}

	@Override
	public void visit(Command streamJitCommand) {
		streamJitCommand.process(cp);
	}

	@Override
	public void visit(SystemInfo systemInfo) {

	}

	@Override
	public void visit(Error error) {
		error.process(ep);
	}

	@Override
	public void visit(Request request) {
		request.process(rp);
	}

	@Override
	public void visit(JsonString json) {
		json.process(jp);
	}
}
