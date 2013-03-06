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

	//Provided for subclasses that want a variable-size argument list.
	protected void addOperand(int i, Value v) {
		//easy case, just add at the end
		if (i == uses.size()) {
			//Don't let the value see a state where we use it as operand i but
			//only have i-1 operands.
			Use use = new Use(this, i, null);
			uses.add(use);
			use.setOperand(v);
		} else {
			//Insertion in the middle requires changing all the operand indexes.
			//Strategy: turn the List<Use> into a List<Value> (removing uses),
			//then add the new value, then recreate the List<Use>.
			throw new UnsupportedOperationException("TODO");
		}
	}

	//Provided for subclasses that want a variable-size argument list.
	protected void removeOperand(int i) {
		//easy case, removing from the end
		if (i == uses.size()-1) {
			Use use = uses.get(i);
			use.setOperand(null);
			uses.remove(i);
		} else {
			//Removal in the middle requires changing all the operand indexes.
			//Strategy: turn the List<Use> into a List<Value> (removing uses),
			//then remove the value, then recreate the List<Use>.
			throw new UnsupportedOperationException("TODO");
		}
	}
}
