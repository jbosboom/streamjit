package edu.mit.streamjit.impl.compiler.insts;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Field;
import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.InstanceFieldType;

/**
 * Loads a static or instance field.  (Does not load array elements.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/14/2013
 */
public class LoadInst extends Instruction {
	public LoadInst(Field f) {
		super(f.getType().getFieldType(), f.isStatic() ? 1 : 2);
		setOperand(0, f);
	}
	public LoadInst(Field f, Value v) {
		this(f);
		setOperand(1, v);
	}

	public Field getField() {
		return (Field)getOperand(0);
	}
	public void setField(Field f) {
		setOperand(0, f);
	}
	public Value getInstance() {
		return getOperand(1);
	}
	public void setInstance(Value v) {
		setOperand(1, v);
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkElementIndex(i, 2);
		if (i == 0)
			checkArgument(v instanceof Field && ((Field)v).isStatic() == getField().isStatic());
		else if (i == 1) {
			checkState(!getField().isStatic());
			checkArgument(v.getType().isSubtypeOf(((InstanceFieldType)getField().getType()).getInstanceType()));
		}
		super.checkOperand(i, v);
	}
}
