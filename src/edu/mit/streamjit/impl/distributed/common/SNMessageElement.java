package edu.mit.streamjit.impl.distributed.common;

public interface SNMessageElement {

	public void accept(SNMessageVisitor visitor);
}
