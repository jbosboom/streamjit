package edu.mit.streamjit.impl.compiler.insts;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.impl.compiler.Method;
import edu.mit.streamjit.impl.compiler.Value;

/**
 * A method call.  All types of bytecoded calls (i.e., not invokedynamic) use
 * this instruction; the opcode to generate is determined by the method being
 * called and the relationship between the instruction's parent class and the
 * method's parent class.
 *
 * TODO: this needs to track the class hierarchy so it can change methods if we
 * add or remove an override.  (Not really a problem for StreamJIT's purposes
 * but annoyingly un-general.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/13/2013
 */
public final class CallInst extends Instruction {
	public CallInst(Method m) {
		super(checkNotNull(m).getType().getReturnType(), 1+m.getType().getParameterTypes().size());
		setOperand(0, m);
	}
	public Method getMethod() {
		return (Method)getOperand(0);
	}
	public void setMethod(Method m) {
		setOperand(0, m);
	}
	public Value getArgument(int i) {
		return getOperand(i+1);
	}
	public void setArgument(int i, Value v) {
		setOperand(i+1, v);
	}
	public Iterable<Value> arguments() {
		return Iterables.skip(operands(), 1);
	}

	@Override
	protected void checkOperand(int i, Value v) {
		if (i == 0)
			checkArgument(v instanceof Method);
		else {
			checkArgument(v.getType().isSubtypeOf(getMethod().getType().getParameterTypes().get(i-1)),
					"cannot assign %s (%s) to parameter type %s",
					v, v.getType(), getMethod().getType().getParameterTypes().get(i-1));
		}
		super.checkOperand(i, v);
	}
}
