package edu.mit.streamjit.impl.distributed.common;

import edu.mit.streamjit.impl.distributed.profiler.SNProfileElement;

public interface SNMessageVisitor {

	void visit(Error error);

	void visit(SystemInfo systemInfo);

	void visit(AppStatus appStatus);

	void visit(NodeInfo nodeInfo);

	void visit(SNDrainElement snDrainElement);

	void visit(SNException snException);

	void visit(SNTimeInfo timeInfo);

	void visit(SNProfileElement snProfileElement);
}