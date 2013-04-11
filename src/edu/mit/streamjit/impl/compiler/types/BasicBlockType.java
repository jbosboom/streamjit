package edu.mit.streamjit.impl.compiler.types;

import edu.mit.streamjit.impl.compiler.Module;

/**
 * The type of a BasicBlock.  Currently, this is a singleton type.  (We might
 * choose to make it per-Method at some point to prevent branch instructions
 * from branching to blocks in other methods, but as a Value's type is
 * immutable, that would mean BasicBlocks would be permanently attached to
 * Functions.  For now we'll preserve the flexibility to transplant BasicBlocks
 * or create free-floating BasicBlocks before inserting them into a Method.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public final class BasicBlockType extends Type {
	private final Module module;
	BasicBlockType(Module module) {
		this.module = module;
	}

	@Override
	public String toString() {
		return "BasicBlock";
	}

	@Override
	protected Module getModule() {
		return module;
	}

	@Override
	protected TypeFactory getTypeFactory() {
		return getModule().types();
	}
}
