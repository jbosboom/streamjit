package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.impl.compiler.types.BasicBlockType;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class BasicBlock extends Value {
	private Method parent;
	public BasicBlock(Method parent) {
		super(BasicBlockType.of());
		parent.add(this);
	}

	public Method getParent() {
		return parent;
	}

	void setParent(Method method) {
		parent = method;
	}
}
