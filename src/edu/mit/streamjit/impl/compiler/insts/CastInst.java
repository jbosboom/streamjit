package edu.mit.streamjit.impl.compiler.insts;

import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.Type;

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
	protected void checkOperand(int i, Value v) {
		//TODO: check the cast is legal/possible
		super.checkOperand(i, v);
	}
}
