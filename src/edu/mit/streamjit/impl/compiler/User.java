package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.impl.compiler.types.Type;
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
	/**
	 * The operands of this User, stored in these "outgoing" Uses.  Not to be
	 * confused with the "incoming" Uses stored in Value and returned by uses().
	 */
	private final List<Use> uses;
	/**
	 * Creates a User of the given type with an empty operand list.
	 * @param type the type of this User
	 */
	public User(Type type) {
		this(type, null);
	}
	/**
	 * Creates a User of the given type with an operand list of the given size,
	 * with all operands initialized to null.
	 * @param type the type of this User
	 * @param operands the number of operands of this User
	 */
	public User(Type type, int operands) {
		this(type, operands, null);
	}
	/**
	 * Creates a User of the given type with an empty operand list.
	 * @param type the type of this User
	 * @param name the name of this User (may be null)
	 */
	public User(Type type, String name) {
		super(type, name);
		uses = new ArrayList<>();
	}
	/**
	 * Creates a User of the given type with an operand list of the given size,
	 * with all operands initialized to null.
	 * @param type the type of this User
	 * @param operands the number of operands of this User
	 * @param name the name of this User (may be null)
	 */
	public User(Type type, int operands, String name) {
		super(type, name);
		uses = new ArrayList<>(operands);
		for (int i = 0; i < operands; ++i)
			uses.add(new Use(this, i, null));
	}

	public int getNumOperands() {
		return uses.size();
	}

	public Iterable<Value> operands() {
		return new Iterable<Value>() {
			@Override
			public Iterator<Value> iterator() {
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
		};
	}

	public Value getOperand(int i) {
		return uses.get(i).getOperand();
	}

	public void setOperand(int i, Value v) {
		uses.get(i).setOperand(v);
	}

	/**
	 * Replaces all uses of the given value with the other given value.  Either
	 * argument may be null.
	 *
	 * When replacements are made, checkOperand() will be called, but note that
	 * some checks may be made after replacements have already occurred.
	 * @param from the value to replace (may be null)
	 * @param to the replacement value (may be null)
	 * @return the number of replacements
	 */
	public int replaceUsesOfWith(Value from, Value to) {
		if (Objects.equals(from, to))
			return 0;
		int replaced = 0;
		for (int i = 0; i < uses.size(); ++i) {
			Use use = uses.get(i);
			if (Objects.equals(use.getOperand(), from)) {
				use.setOperand(to);
				++replaced;
			}
		}
		return replaced;
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
