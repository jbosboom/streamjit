package edu.mit.streamjit.impl.compiler.insts;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import static com.google.common.base.Preconditions.*;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.impl.compiler.Method;
import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.PrimitiveType;
import edu.mit.streamjit.impl.compiler.types.RegularType;
import edu.mit.streamjit.impl.compiler.types.VoidType;
import java.util.Map;

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
	public CallInst(Method m, Value... arguments) {
		this(m);
		for (int i = 0; i < arguments.length; ++i)
			setArgument(i, arguments[i]);
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
	public CallInst clone(Function<Value, Value> operandMap) {
		CallInst ci = new CallInst((Method)operandMap.apply(getMethod()));
		for (int i = 1; i < getNumOperands(); ++i)
			ci.setOperand(i, operandMap.apply(getOperand(i)));
		return ci;
	}

	@Override
	protected void checkOperand(int i, Value v) {
		if (i == 0)
			checkArgument(v instanceof Method);
		else {
			RegularType paramType = getMethod().getType().getParameterTypes().get(i-1);
			PrimitiveType intType = paramType.getTypeFactory().getPrimitiveType(int.class);
			//Due to the JVM's type system not distinguishing types smaller than
			//int, we can implicitly convert to int then to the parameter type.
			//Otherwise, we need a subtype match.
			if (!(v.getType().isSubtypeOf(intType) && paramType.isSubtypeOf(intType)))
				checkArgument(v.getType().isSubtypeOf(paramType),
						"cannot assign %s (%s) to parameter type %s",
						v, v.getType(), getMethod().getType().getParameterTypes().get(i-1));
		}
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(getName());
		if (getType() instanceof VoidType)
			sb.append(": ");
		else
			sb.append(" (").append(getType()).append(") = ");
		sb.append("call ").append(getMethod().getParent().getName()).append("#").append(getMethod().getName());
		sb.append("(");
		Joiner.on(", ").appendTo(sb, FluentIterable.from(arguments()).transform(new Function<Value, String>() {
			@Override
			public String apply(Value input) {
				return input.getName();
			}
		}));
		sb.append(")");
		return sb.toString();
	}
}
