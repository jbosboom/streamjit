package edu.mit.streamjit.util.bytecode;

import edu.mit.streamjit.util.ReflectionUtils;
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
	private int operandIndex;
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

	//for internal use only!
	void setOperandIndex(int index) {
		assert ReflectionUtils.calledDirectlyFrom(User.class);
		operandIndex = index;
	}

	public Value getOperand() {
		return value;
	}

	public void setOperand(Value other) {
		user.checkOperandInternal(operandIndex, other);
		if (Objects.equals(getOperand(), other))
			return;
		if (value != null)
			value.removeUse(this);
		this.value = other;
		if (other != null)
			other.addUse(this);
	}

	@Override
	public String toString() {
		return "Use{" + "user=" + user + ", operandIndex=" + operandIndex + '}';
	}
}
