package edu.mit.streamjit.impl.distributed.common;

public interface SNMessageVisitor {

	void visit(Error error);

	void visit(SystemInfo systemInfo);

	void visit(AppStatus appStatus);

	void visit(NodeInfo nodeInfo);

	void visit(SNDrainElement snDrainElement);

	void visit(SNException snException);

	void visit(SNTimeInfo timeInfo);
}