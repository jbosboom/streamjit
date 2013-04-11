package edu.mit.streamjit.impl.compiler.insts;

import edu.mit.streamjit.impl.compiler.BasicBlock;
import edu.mit.streamjit.impl.compiler.ParentedList;
import edu.mit.streamjit.impl.compiler.User;
import edu.mit.streamjit.impl.compiler.types.Type;
import edu.mit.streamjit.util.IntrusiveList;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/11/2013
 */
public abstract class Instruction extends User implements ParentedList.Parented<BasicBlock> {
	@IntrusiveList.Previous
	private Instruction previous;
	@IntrusiveList.Next
	private Instruction next;
	@ParentedList.Parent
	private BasicBlock parent;

	protected Instruction(Type type) {
		super(type);
	}

	protected Instruction(Type type, int operands) {
		super(type, operands);
	}

	protected Instruction(Type type, String name) {
		super(type, name);
	}

	protected Instruction(Type type, int operands, String name) {
		super(type, operands, name);
	}

	@Override
	public BasicBlock getParent() {
		return parent;
	}
}
