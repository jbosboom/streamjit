package edu.mit.streamjit.impl.compiler.insts;

import edu.mit.streamjit.impl.compiler.BasicBlock;
import edu.mit.streamjit.impl.compiler.ParentedList;
import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.Type;
import edu.mit.streamjit.util.IntrusiveList;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/11/2013
 */
public abstract class Instruction extends Value implements ParentedList.Parented<BasicBlock> {
	@IntrusiveList.Previous
	private Instruction previous;
	@IntrusiveList.Next
	private Instruction next;
	@ParentedList.Parent
	private BasicBlock parent;

	protected Instruction(Type type) {
		super(type);
	}
	protected Instruction(Type type, String name) {
		super(type, name);
	}

	@Override
	public BasicBlock getParent() {
		return parent;
	}
}
