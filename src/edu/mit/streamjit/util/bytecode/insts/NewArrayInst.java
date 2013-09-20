package edu.mit.streamjit.util.bytecode.insts;

import com.google.common.base.Function;
import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.util.bytecode.Value;
import edu.mit.streamjit.util.bytecode.types.ArrayType;

/**
 * Creates a primitive or reference array, initializing at least one of its
 * dimensions.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/16/2013
 */
public final class NewArrayInst extends Instruction {
	public NewArrayInst(ArrayType type, int dimensionsToCreate) {
		super(type, dimensionsToCreate);
		checkArgument(dimensionsToCreate >= 1);
		checkArgument(dimensionsToCreate <= type.getDimensions());
	}
	public NewArrayInst(ArrayType type, Value... dimensions) {
		super(type, dimensions.length);
		for (int i = 0; i < dimensions.length; ++i)
			setOperand(i, dimensions[i]);
	}

	@Override
	public ArrayType getType() {
		return (ArrayType)super.getType();
	}

	@Override
	public Instruction clone(Function<Value, Value> operandMap) {
		Value[] dimensions = new Value[getNumOperands()];
		for (int i = 0; i < getNumOperands(); ++i)
			dimensions[i] = operandMap.apply(getOperand(i));
		return new NewArrayInst(getType(), dimensions);
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkArgument(v.getType().isSubtypeOf(v.getType().getTypeFactory().getType(int.class)));
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(getName());
		sb.append(" (").append(getType()).append(") = new ").append(getType().getElementType());
		for (int i = 0; i < getType().getDimensions(); ++i) {
			sb.append('[');
			if (i < getNumOperands())
				sb.append(getOperand(i).getName());
			sb.append(']');
		}
		return sb.toString();
	}
}
