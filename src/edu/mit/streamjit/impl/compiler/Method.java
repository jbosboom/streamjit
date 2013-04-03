package edu.mit.streamjit.impl.compiler;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
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
public class Method extends Value implements ParentedList.Parented<Module> {
	//Intrusive list fields.
	private Method next, previous;
	private Module parent;
	private final ImmutableList<Argument> arguments;
	public Method(MethodType type, String name, Module parent) {
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
	public Module getParent() {
		return parent;
	}

	void setParent(Module module) {
		parent = module;
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

	private static final class MethodSupport implements ParentedList.Support<Module, Method> {
		@Override
		public Module setParent(Method t, Module newParent) {
			Module parent = t.getParent();
			t.parent = newParent;
			return parent;
		}
		@Override
		public Method getPrevious(Method t) {
			return t.previous;
		}
		@Override
		public Method setPrevious(Method t, Method newPrevious) {
			Method previous = getPrevious(t);
			t.previous = newPrevious;
			return previous;
		}
		@Override
		public Method getNext(Method t) {
			return t.next;
		}
		@Override
		public Method setNext(Method t, Method newNext) {
			Method next = getNext(t);
			t.next = newNext;
			return next;
		}
	}
	static {
		ParentedList.registerSupport(Method.class, new MethodSupport());
	}
}
