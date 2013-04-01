package edu.mit.streamjit.impl.compiler;

import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A User is a Value that can use other Values as operands.
 *
 * By default, this class implements a fixed-size operand list.  Subclasses that
 * want a variable-size list should override addOperand and/or removeOperand
 * as public or call them from their implementation.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public abstract class User extends Value {
	private final List<Use> uses;
	private final List<Value> operands;
	public User(Type type, int numOperands) {
		super(type);
		this.uses = new ArrayList<>(numOperands);
		this.operands = new ArrayList<>(numOperands);
		for (int i = 0; i < numOperands; ++i) {
			operands.add(null);
			uses.add(new Use(this, i));
		}
	}
	public User(Type type, Value... values) {
		this(type, values.length);
		for (int i = 0; i < values.length; ++i)
			setOperand(i, values[i]);
	}

	public int getNumOperands() {
		return operands.size();
	}

	public Iterator<Value> operandIterator() {
		return Iterators.unmodifiableIterator(operands.iterator());
	}

	public Value getOperand(int i) {
		return operands.get(i);
	}

	public void setOperand(int i, Value newOperand) {
		Value oldOperand = getOperand(i);
		if (Objects.equals(oldOperand, newOperand))
			return;
		Use use = uses.get(i);
		if (oldOperand != null)
			oldOperand.removeUse(use);
		operands.set(i, newOperand);
		if (newOperand != null)
			newOperand.addUse(use);
	}

	//Provided for subclasses that want a variable-size operand list.
	protected void addOperand(int i, Value v) {
		//Must add operand before use so usee doesn't think it's an error.
		operands.add(i, v);
		uses.add(i, new Use(this, i));
		for (int j = i+1; i < uses.size(); ++i)
			uses.get(j).setOperandIndex(j);
	}

	//Provided for subclasses that want a variable-size operand list.
	protected void removeOperand(int i) {
		//Must remove use before operand so usee doesn't think it's an error.
		getOperand(i).removeUse(uses.remove(i));
		operands.remove(i);
		for (; i < uses.size(); ++i)
			uses.get(i).setOperandIndex(i);
	}
}
