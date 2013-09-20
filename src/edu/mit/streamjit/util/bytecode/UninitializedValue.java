package edu.mit.streamjit.util.bytecode;

import edu.mit.streamjit.util.bytecode.types.Type;

/**
 * An uninitialized or otherwise unimportant value.  Used for the this parameter
 * inside constructors and for uninitialized objects on the operand stack (after
 * a new opcode but before the constructor call), and possibly other places.
 *
 * Has the type of the object being initialized.
 */
public final class UninitializedValue extends Value {
	public UninitializedValue(Type type, String name) {
		super(type, name);
	}
}
