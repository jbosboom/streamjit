package edu.mit.streamjit.impl.distributed.common;

public class SNException implements SNMessageElement{

	@Override
	public void accept(SNMessageVisitor visitor) {
		visitor.visit(this);
	}

}
