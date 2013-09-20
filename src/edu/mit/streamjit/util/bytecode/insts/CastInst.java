package edu.mit.streamjit.util.bytecode.insts;

import com.google.common.base.Function;
import edu.mit.streamjit.util.bytecode.Value;
import edu.mit.streamjit.util.bytecode.types.Type;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/15/2013
 */
public final class CastInst extends Instruction {
	public CastInst(Type targetType) {
		super(targetType, 1);
	}
	public CastInst(Type targetType, Value source) {
		this(targetType);
		setOperand(0, source);
	}

	@Override
	public CastInst clone(Function<Value, Value> operandMap) {
		return new CastInst(getType(), operandMap.apply(getOperand(0)));
	}

	@Override
	protected void checkOperand(int i, Value v) {
		//TODO: check the cast is legal/possible
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		return String.format("%s (%s) = cast %s to %s",
				getName(), getType(), getOperand(0).getName(), getType());
	}
}
