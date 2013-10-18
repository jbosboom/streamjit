package edu.mit.streamjit.impl.distributed.common;

import java.io.Serializable;

public interface SNMessageElement extends Serializable {

	public void accept(SNMessageVisitor visitor);
}
