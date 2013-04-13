package edu.mit.streamjit.impl.compiler;

import com.google.common.collect.ImmutableSet;
import edu.mit.streamjit.impl.compiler.insts.Instruction;
import edu.mit.streamjit.impl.compiler.insts.TerminatorInst;
import edu.mit.streamjit.impl.compiler.types.BasicBlockType;
import edu.mit.streamjit.util.IntrusiveList;
import java.util.Collections;
import java.util.Iterator;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class BasicBlock extends Value implements Parented<Method> {
	@IntrusiveList.Previous
	private BasicBlock previous;
	@IntrusiveList.Next
	private BasicBlock next;
	@ParentedList.Parent
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
	public BasicBlockType getType() {
		return (BasicBlockType)super.getType();
	}

	@Override
	public Method getParent() {
		return parent;
	}

	public TerminatorInst getTerminator() {
		if (instructions.isEmpty())
			return null;
		Instruction lastInst = instructions.listIterator(instructions.size()).previous();
		return lastInst instanceof TerminatorInst ? (TerminatorInst)lastInst : null;
	}

	public Iterable<BasicBlock> predecessors() {
		return new Iterable<BasicBlock>() {
			@Override
			public Iterator<BasicBlock> iterator() {
				ImmutableSet.Builder<BasicBlock> builder = ImmutableSet.builder();
				for (User user : users().elementSet())
					if (user instanceof TerminatorInst && ((Instruction)user).getParent() != null)
						builder.add(((Instruction)user).getParent());
				return builder.build().iterator();
			}
		};
	}

	public Iterable<BasicBlock> successors() {
		TerminatorInst terminator = getTerminator();
		return terminator != null ? terminator.successors() : Collections.<BasicBlock>emptyList();
	}
}
