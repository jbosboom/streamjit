package edu.mit.streamjit.impl.distributed.common;

/**
 * Visitor pattern. We have to have overloaded visit method to all sub type of
 * {@link MessageElement}s. See the {@link MessageElement}.
 * 
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
public interface CTRLRMessageVisitor {

	public void visit(Command command);

	public void visit(Request request);

	public void visit(ConfigurationString json);

	public void visit(CTRLRDrainElement ctrlrDrainElement);

	public void visit(MiscCtrlElements miscCtrlElements);
}
