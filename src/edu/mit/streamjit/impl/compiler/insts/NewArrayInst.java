package edu.mit.streamjit.impl.compiler.insts;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.ArrayType;

/**
 * Creates a primitive or reference array, initializing at least one of its
 * dimensions.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/16/2013
 */
public class NewArrayInst extends Instruction {
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
	protected void checkOperand(int i, Value v) {
		checkArgument(v.getType().isSubtypeOf(v.getType().getTypeFactory().getType(int.class)));
		super.checkOperand(i, v);
	}
}
