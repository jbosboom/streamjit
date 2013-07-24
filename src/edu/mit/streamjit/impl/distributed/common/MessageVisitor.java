package edu.mit.streamjit.impl.distributed.common;

/**
 * Visitor pattern. See the {@link MessageElement}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
public interface MessageVisitor {

	public void visit(AppStatus appStatus);

	public void visit(Command command);

	public void visit(Error error);

	public void visit(Request request);

	public void visit(JsonString json);

	public void visit(SystemInfo systemInfo);

	public void visit(NodeInfo nodeInfo);

}
