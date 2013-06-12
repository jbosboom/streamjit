/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.api;


public interface MessageVisitor {

	public void visit(AppStatus appStatus);

	public void visit(Command command);

	public void visit(SystemInfo systemInfo);

	public void visit(Error error);

	public void visit(Request request);
	
	public void visit(JsonString json);
}
