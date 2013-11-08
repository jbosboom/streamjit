package edu.mit.streamjit.util.bytecode;

import edu.mit.streamjit.util.Parented;
import edu.mit.streamjit.util.ParentedList;
import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import edu.mit.streamjit.util.bytecode.insts.Instruction;
import edu.mit.streamjit.util.bytecode.insts.TerminatorInst;
import edu.mit.streamjit.util.bytecode.types.BasicBlockType;
import edu.mit.streamjit.util.IntrusiveList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

	public BasicBlock(Module module, String name) {
		this(module);
		setName(name);
	}

	@Override
	public BasicBlockType getType() {
		return (BasicBlockType)super.getType();
	}

	@Override
	public Method getParent() {
		return parent;
	}

	public List<Instruction> instructions() {
		//TODO: figure out how to make this immutable when the parent is
		//immutable.  Note that we add to this list during resolution.
		return instructions;
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

	public BasicBlock removeFromParent() {
		checkState(getParent() != null);
		getParent().basicBlocks().remove(this);
		return this;
	}

	public void eraseFromParent() {
		removeFromParent();
		for (Instruction i : ImmutableList.copyOf(instructions()))
			i.eraseFromParent();
	}
}
