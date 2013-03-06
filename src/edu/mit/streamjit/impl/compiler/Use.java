package edu.mit.streamjit.impl.compiler;

import java.util.Objects;

/**
 * A Use represents a single use of a Value.  Note that a User can use the same
 * value more than once if it has multiple operands; this class thus keeps an
 * operand index to disambiguate.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class Use {
	private final User user;
	private final int operandIndex;
	private Value value;

	public Use(User user, int operandIndex, Value value) {
		this.user = user;
		this.operandIndex = operandIndex;
		this.value = value;
		if (value != null)
			value.addUse(this);
	}

	public User getUser() {
		return user;
	}

	public int getOperandIndex() {
		return operandIndex;
	}

	public Value getOperand() {
		return value;
	}

	public void setOperand(Value other) {
		if (Objects.equals(getOperand(), other))
			return;
		if (value != null)
			value.removeUse(this);
		this.value = other;
		if (other != null)
			other.addUse(this);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final Use other = (Use)obj;
		if (!Objects.equals(this.user, other.user))
			return false;
		if (this.operandIndex != other.operandIndex)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 97 * hash + Objects.hashCode(this.user);
		hash = 97 * hash + this.operandIndex;
		return hash;
	}

	@Override
	public String toString() {
		return "Use{" + "user=" + user + ", operandIndex=" + operandIndex + '}';
	}
}
