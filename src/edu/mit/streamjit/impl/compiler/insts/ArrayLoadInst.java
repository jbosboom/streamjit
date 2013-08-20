package edu.mit.streamjit.impl.compiler.insts;

import com.google.common.base.Function;
import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.ArrayType;

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
	public ArrayLoadInst clone(Function<Value, Value> operandMap) {
		return new ArrayLoadInst(operandMap.apply(getArray()), operandMap.apply(getIndex()));
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

	@Override
	public String toString() {
		return String.format("%s (%s) = arrayload %s [%s]",
				getName(), getType(), getOperand(0).getName(), getOperand(1).getName());
	}
}
