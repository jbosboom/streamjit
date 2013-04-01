package edu.mit.streamjit.impl.compiler;

/**
 * A Use represents a single use of a Value.  Note that a User can use the same
 * value more than once if it has multiple operands; this class thus keeps an
 * operand index to disambiguate.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class Use {
	private final User user;
	private int operandIndex;

	public Use(User user, int operandIndex) {
		this.user = user;
		this.operandIndex = operandIndex;
	}

	public User getUser() {
		return user;
	}

	public int getOperandIndex() {
		return operandIndex;
	}

	public void setOperandIndex(int index) {
		operandIndex = index;
	}

	public Value getOperand() {
		return user.getOperand(operandIndex);
	}

	public void setOperand(Value other) {
		user.setOperand(operandIndex, other);
	}

	@Override
	public String toString() {
		return "Use{" + "user=" + user + ", operandIndex=" + operandIndex + '}';
	}
}
