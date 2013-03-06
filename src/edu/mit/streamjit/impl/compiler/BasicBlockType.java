package edu.mit.streamjit.impl.compiler;

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
public class BasicBlockType extends Type {
	private static final BasicBlockType INSTANCE = new BasicBlockType();
	private BasicBlockType() {}
	public static BasicBlockType of() {
		return INSTANCE;
	}

	@Override
	public String toString() {
		return "BasicBlock";
	}
}
