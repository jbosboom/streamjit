package edu.mit.streamjit.impl.compiler.insts;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.impl.compiler.BasicBlock;
import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.Type;
import java.util.Iterator;

/**
 * A phi instruction resolves conflicting definitions from predecessor predecessors.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/15/2013
 */
public final class PhiInst extends Instruction {
	public PhiInst(Type type) {
		super(type);
	}

	public Value get(BasicBlock b) {
		int bbi = Iterables.indexOf(operands(), Predicates.<Value>equalTo(b));
		return bbi != -1 ? getOperand(bbi+1) : null;
	}

	public Value put(BasicBlock b, Value v) {
		checkNotNull(b);
		checkNotNull(v);
		checkArgument(v.getType().isSubtypeOf(getType()), "%s not a %s", v, getType());
		int bbi = Iterables.indexOf(operands(), Predicates.<Value>equalTo(b));
		Value oldVal = get(b);
		if (bbi != -1)
			setOperand(bbi+1, v);
		else {
			addOperand(getNumOperands(), b);
			addOperand(getNumOperands(), v);
		}
		return oldVal;
	}

	public Iterable<BasicBlock> predecessors() {
		return FluentIterable.from(operands()).filter(BasicBlock.class);
	}

	public Iterable<Value> incomingValues() {
		return FluentIterable.from(operands()).filter(Value.class);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(getName());
		sb.append(" (").append(getType()).append(") = phi ");
		for (Iterator<Value> i = operands().iterator(); i.hasNext();)
			sb.append("[").append(i.next().getName()).append(", ").append(i.next().getName()).append("], ");
		sb.delete(sb.length()-2, sb.length());
		return sb.toString();
	}
}
