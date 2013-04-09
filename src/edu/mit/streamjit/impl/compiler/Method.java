package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.impl.compiler.types.RegularType;
import edu.mit.streamjit.impl.compiler.types.MethodType;
import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.util.IntrusiveList;
import java.util.Iterator;
import java.util.List;

/**
 * Method represents an executable element of a class file (instance method,
 * class (static) method, instance initializer (constructor), or class (static)
 * initializer).
 *
 * Methods may may be internal (with basic blocks) or external (without).
 * Internal methods will be emitted in the output, while external methods will
 * only be referenced by call instructions.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class Method extends Value implements ParentedList.Parented<Klass> {
	@IntrusiveList.Next
	private Method next;
	@IntrusiveList.Previous
	private Method previous;
	@ParentedList.Parent
	private Klass parent;

	private final ImmutableList<Argument> arguments;
	public Method(MethodType type, String name, Klass parent) {
		super(type, name);
		ImmutableList.Builder<Argument> builder = ImmutableList.builder();
		for (Iterator<RegularType> it = type.argumentTypeIterator(); it.hasNext();)
			builder.add(new Argument(this, it.next()));
		this.arguments = builder.build();
		parent.add(this);
	}

	@Override
	public MethodType getType() {
		return (MethodType)super.getType();
	}

	@Override
	public Klass getParent() {
		return parent;
	}

	public ImmutableList<Argument> arguments() {
		return arguments;
	}

	public void add(BasicBlock block) {
		basicBlocks.add(block);
		block.setParent(this);
	}

	public void remove(BasicBlock block) {
		basicBlocks.remove(block);
		block.setParent(null);
	}

	public List<BasicBlock> basicBlocks() {
		return basicBlocks;
	}
}
