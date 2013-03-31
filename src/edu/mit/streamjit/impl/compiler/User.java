package edu.mit.streamjit.impl.compiler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
	public User(Type type, int operands) {
		super(type);
		uses = new ArrayList<>(operands);
		for (int i = 0; i < operands; ++i)
			uses.add(new Use(this, i, null));
	}
	public User(Type type, Value... values) {
		this(type, values.length);
		for (int i = 0; i < values.length; ++i)
			setOperand(i, values[i]);
	}

	public int getNumOperands() {
		return uses.size();
	}

	public Iterator<Value> operandIterator() {
		return new Iterator<Value>() {
			private final Iterator<Use> iter = uses.iterator();
			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}
			@Override
			public Value next() {
				return iter.next().getOperand();
			}
			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	public Value getOperand(int i) {
		return uses.get(i).getOperand();
	}

	public void setOperand(int i, Value v) {
		uses.get(i).setOperand(v);
	}

	//Provided for subclasses that want a variable-size operand list.
	protected void addOperand(int i, Value v) {
		Use use = new Use(this, i, null);
		uses.add(i, use);
		for (int j = i+1; i < uses.size(); ++i)
			uses.get(j).setOperandIndex(j);
		use.setOperand(v);
	}

	//Provided for subclasses that want a variable-size operand list.
	protected void removeOperand(int i) {
		Use use = uses.get(i);
		use.setOperand(null);
		uses.remove(i);
		for (; i < uses.size(); ++i)
			uses.get(i).setOperandIndex(i);
	}
}
