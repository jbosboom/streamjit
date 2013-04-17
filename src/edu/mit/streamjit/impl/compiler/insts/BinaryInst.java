package edu.mit.streamjit.impl.compiler.insts;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.PrimitiveType;

/**
 * A binary mathematical operation.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/15/2013
 */
public final class BinaryInst extends Instruction {
	public enum Operation {
		ADD("int", "long", "float", "double"),
		SUB("int", "long", "float", "double"),
		MUL("int", "long", "float", "double"),
		DIV("int", "long", "float", "double"),
		REM("int", "long", "float", "double"),
		SHL("int", "long"),
		SHR("int", "long"),
		USHR("int", "long"),
		AND("int", "long"),
		OR("int", "long"),
		XOR("int", "long"),
		CMP("long", "float", "double"),
		CMPG("float", "double");
		private final ImmutableList<String> types;
		private Operation(String... types) {
			this.types = ImmutableList.copyOf(types);
		}
		public ImmutableList<String> applicableTypes() {
			return types;
		}
	}

	private final Operation operation;
	public BinaryInst(Value left, Operation op, Value right) {
		super(computeType(left, op, right), 2);
		if (op == Operation.CMP || op == Operation.CMPG)
			checkArgument(op.applicableTypes().contains(left.getType().toString()) &&
					op.applicableTypes().contains(left.getType().toString()),
					"%s %s %s", left.getType(), op, right.getType());
		else
			checkArgument(op.applicableTypes().contains(getType().toString()), "%s %s", op, getType());
		setOperand(0, left);
		setOperand(1, right);
		this.operation = op;
	}

	public Operation getOperation() {
		return operation;
	}

	private static PrimitiveType computeType(Value left, Operation operation, Value right) {
		PrimitiveType intType = left.getType().getTypeFactory().getPrimitiveType(int.class);
		//Comparisons are always int.  (TODO: byte?)
		if (operation == Operation.CMP || operation == Operation.CMPG)
			return intType;
		//If both promotable to int, result is int.
		if (left.getType().isSubtypeOf(intType) && right.getType().isSubtypeOf(intType))
			return intType;
		//Else types must be primitive and equal.
		if (left.getType().equals(right.getType()) && left.getType() instanceof PrimitiveType)
			return (PrimitiveType)left.getType();
		throw new IllegalArgumentException("type mismatch: "+left+" "+operation+" "+right);
	}

	@Override
	public String toString() {
		return String.format("%s (%s) = %s %s, %s",
				getName(), getType(), getOperation(),
				getOperand(0).getName(), getOperand(1).getName());
	}
}
