/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.slave;

import edu.mit.streamjit.impl.distributed.runtime.api.AppStatus;
import edu.mit.streamjit.impl.distributed.runtime.api.AppStatusProcessor;
import edu.mit.streamjit.impl.distributed.runtime.api.Command;
import edu.mit.streamjit.impl.distributed.runtime.api.CommandProcessor;
import edu.mit.streamjit.impl.distributed.runtime.api.Error;
import edu.mit.streamjit.impl.distributed.runtime.api.ErrorProcessor;
import edu.mit.streamjit.impl.distributed.runtime.api.JsonString;
import edu.mit.streamjit.impl.distributed.runtime.api.JsonStringProcessor;
import edu.mit.streamjit.impl.distributed.runtime.api.MessageVisitor;
import edu.mit.streamjit.impl.distributed.runtime.api.Request;
import edu.mit.streamjit.impl.distributed.runtime.api.RequestProcessor;
import edu.mit.streamjit.impl.distributed.runtime.api.SystemInfo;

public class SlaveMessageVisitor implements MessageVisitor {

	private AppStatusProcessor asp;
	private CommandProcessor cp;
	private ErrorProcessor ep;
	private RequestProcessor rp;
	private JsonStringProcessor jp;

	public SlaveMessageVisitor(AppStatusProcessor asp, CommandProcessor cp, ErrorProcessor ep, RequestProcessor rp, JsonStringProcessor jp) {
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
