package edu.mit.streamjit.impl.compiler.insts;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.ArrayType;
import edu.mit.streamjit.impl.compiler.types.NullType;
import edu.mit.streamjit.impl.compiler.types.RegularType;
import edu.mit.streamjit.impl.compiler.types.Type;

/**
 * Stores a value in an array.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/16/2013
 */
public final class ArrayLoadInst extends Instruction {
	public ArrayLoadInst(Value array, Value index) {
		super(((ArrayType)array.getType()).getComponentType(), 2);
		setOperand(0, array);
		setOperand(1, index);
	}

	public Value getArray() {
		return getOperand(0);
	}
	public void setArray(Value v) {
		setOperand(0, v);
	}
	public Value getIndex() {
		return getOperand(1);
	}
	public void setIndex(Value v) {
		setOperand(1, v);
	}

	@Override
	protected void checkOperand(int i, Value v) {
		if (i == 0) {
			checkArgument(v.getType() instanceof ArrayType);
			checkArgument(((ArrayType)v.getType()).getComponentType().isSubtypeOf(getType()));
		} else if (i == 1)
			checkArgument(v.getType().isSubtypeOf(getType().getTypeFactory().getType(int.class)));
		super.checkOperand(i, v);
	}
}
