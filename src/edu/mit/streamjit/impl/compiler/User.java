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
		//Check before committing any changes, for debuggability.
		checkOperand(i, v);
		for (int j = i; i < uses.size(); ++i)
			checkOperand(j+1, uses.get(j).getOperand());

		Use use = new Use(this, i, null);
		uses.add(i, use);
		for (int j = i+1; i < uses.size(); ++i)
			uses.get(j).setOperandIndex(j);
		use.setOperand(v);
	}

	//Provided for subclasses that want a variable-size operand list.
	protected void removeOperand(int i) {
		//Check before committing any changes, for debuggability.
		for (int j = i; i < uses.size(); ++i)
			checkOperand(j, uses.get(j+1).getOperand());

		Use use = uses.get(i);
		use.setOperand(null);
		uses.remove(i);
		for (; i < uses.size(); ++i)
			uses.get(i).setOperandIndex(i);
	}

	/**
	 * Called before the given value is set at the given operand index.
	 * Subclasses that wish to enforce invariants on their operands can throw
	 * exceptions from this method if setting the operand would violate an
	 * invariant.
	 * <p/>
	 * Operand adds and removes are considered sets of every operand index that
	 * would change (even if it's changing to the same value). For example, an
	 * add at index 3 will result in checks for (3, new operand), (4, old
	 * operand 3), (5, old operand 4), etc.; note that the last check will come
	 * at an invalid index, as the add has not yet occurred. A remove would
	 * check (3, old operand 4), (4, old operand 5), etc. No changes are applied
	 * until all checks pass, though the process of applying the changes may
	 * generate additional checks with the same arguments.
	 * @param i the index of the operand being set
	 * @param v the value the operand is being set to (may be null)
	 */
	protected void checkOperand(int i, Value v) {
	}
}
