package edu.mit.streamjit.impl.compiler.insts;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.ReferenceType;

/**
 * Tests if an object is of a particular type.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/15/2013
 */
public final class InstanceofInst extends Instruction {
	private final ReferenceType testType;
	public InstanceofInst(ReferenceType testType) {
		super(testType.getTypeFactory().getPrimitiveType(boolean.class), 1);
		this.testType = testType;
	}
	public InstanceofInst(ReferenceType testType, Value v) {
		this(testType);
		setOperand(0, v);
	}

	public ReferenceType getTestType() {
		return testType;
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkElementIndex(i, 1);
		checkArgument(v.getType().isSubtypeOf(v.getType().getTypeFactory().getType(Object.class)));
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		return String.format("%s (%s) = %s instanceof %s",
				getName(), getType(), getOperand(0).getName(), testType);
	}
}
