package edu.mit.streamjit.impl.compiler.insts;

import com.google.common.base.Function;
import static com.google.common.base.Preconditions.*;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.impl.compiler.BasicBlock;
import edu.mit.streamjit.impl.compiler.Constant;
import edu.mit.streamjit.impl.compiler.Value;
import java.util.Iterator;

/**
 * Transfers control to one of several possible blocks by comparing a value
 * against a map of integer constants to blocks.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/15/2013
 */
public final class SwitchInst extends TerminatorInst {
	public SwitchInst(Value expr, BasicBlock defaultBlock) {
		super(defaultBlock.getType().getTypeFactory(), 2);
		setValue(expr);
		setDefault(defaultBlock);
	}

	public Value getValue() {
		return getOperand(0);
	}
	public void setValue(Value v) {
		setOperand(0, v);
	}

	public BasicBlock getDefault() {
		return (BasicBlock)getOperand(1);
	}

	public void setDefault(BasicBlock bb) {
		setOperand(1, bb);
	}

	public BasicBlock get(Constant<Integer> cst) {
		int ci = Iterables.indexOf(operands(), Predicates.<Value>equalTo(cst));
		return ci != -1 ? (BasicBlock)getOperand(ci+1) : null;
	}

	public BasicBlock put(Constant<Integer> cst, BasicBlock bb) {
		BasicBlock oldVal = get(cst);
		int ci = Iterables.indexOf(operands(), Predicates.<Value>equalTo(cst));
		if (ci != -1)
			setOperand(ci+1, bb);
		else {
			addOperand(Iterables.size(operands()), cst);
			addOperand(Iterables.size(operands()), bb);
		}
		return oldVal;
	}

	@SuppressWarnings("unchecked")
	public Iterable<Constant<Integer>> cases() {
		return (Iterable<Constant<Integer>>)(Iterable)FluentIterable.from(operands()).filter(Constant.class);
	}

	@Override
	public SwitchInst clone(Function<Value, Value> operandMap) {
		SwitchInst i = new SwitchInst(operandMap.apply(getValue()), (BasicBlock)operandMap.apply(getDefault()));
		for (Constant<Integer> c : cases())
			i.put(((Constant<?>)(operandMap.apply(c))).as(Integer.class), (BasicBlock)operandMap.apply(get(c)));
		return i;
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkArgument(v instanceof BasicBlock || v.getType().isSubtypeOf(v.getType().getTypeFactory().getType(int.class)));
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(getName());
		sb.append(": = switch ").append(getValue().getName());
		sb.append(" default ").append(getDefault().getName()).append(" ");
		for (Iterator<Value> i = operands().iterator(); i.hasNext();)
			sb.append("[").append(i.next().getName()).append(", ").append(i.next().getName()).append("], ");
		sb.delete(sb.length()-2, sb.length());
		return sb.toString();
	}
}
