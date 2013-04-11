package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.impl.compiler.insts.Instruction;
import edu.mit.streamjit.util.IntrusiveList;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class BasicBlock extends Value implements ParentedList.Parented<Method> {
	private Method parent;
	private final IntrusiveList<Instruction> instructions = new ParentedList<>(this, Instruction.class);
	/**
	 * Creates a new, empty BasicBlock not attached to any parent.  The Module
	 * is used to get the correct BasicBlockType.
	 * @param module the module this BasicBlock is associated with
	 */
	public BasicBlock(Module module) {
		super(module.types().getBasicBlockType());
	}

	@Override
	public Method getParent() {
		return parent;
	}
}
