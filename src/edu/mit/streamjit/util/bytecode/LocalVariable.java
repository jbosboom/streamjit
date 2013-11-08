package edu.mit.streamjit.util.bytecode;

import edu.mit.streamjit.util.Parented;
import edu.mit.streamjit.util.ParentedList;
import edu.mit.streamjit.util.bytecode.types.FieldType;
import edu.mit.streamjit.util.bytecode.types.RegularType;

/**
 * Represents a local variable allocated in a method's stack frame which can be
 * loaded or stored and is not subject to SSA form.  This maps directly to the
 * JVM's notion of local variables, except that LocalVariables do not change
 * type on a write like the JVM's local variables do.  LocalVariables are
 * analogous to LLVM's allocas-in-the-entry-block pattern.
 *
 * LocalVariables have field type; use the load and store instructions (without
 * an instance reference) to access their values.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 6/13/2013
 */
public class LocalVariable extends Value implements Parented<Method> {
	@ParentedList.Previous
	private LocalVariable previous;
	@ParentedList.Next
	private LocalVariable next;
	@ParentedList.Parent
	private Method parent;

	public LocalVariable(RegularType type, String name, Method parent) {
		super(type.getTypeFactory().getFieldType(type), name);
		if (parent != null)
			parent.localVariables().add(this);
	}

	@Override
	public FieldType getType() {
		return (FieldType)super.getType();
	}

	@Override
	public Method getParent() {
		return parent;
	}

	@Override
	public String toString() {
		return String.format("%s %s",
				getType().getFieldType(),
				getName());
	}
}
