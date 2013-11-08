package edu.mit.streamjit.util.bytecode.insts;

import com.google.common.base.Function;
import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.util.bytecode.BasicBlock;
import edu.mit.streamjit.util.bytecode.Value;

/**
 * An unconditional jump.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/11/2013
 */
public final class JumpInst extends TerminatorInst {
	public JumpInst(BasicBlock target) {
		super(target.getType().getTypeFactory(), 1);
		setOperand(0, target);
	}

	@Override
	public JumpInst clone(Function<Value, Value> operandMap) {
		return new JumpInst((BasicBlock)operandMap.apply(getOperand(0)));
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkArgument(i == 0, i);
		checkArgument(v instanceof BasicBlock, v.toString());
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		return String.format("%s: goto %s", getName(), getOperand(0));
	}
}
