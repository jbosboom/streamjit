package edu.mit.streamjit.impl.compiler.insts;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Field;
import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.InstanceFieldType;

/**
 * Stores a static or instance field.  (Does not store array elements.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/14/2013
 */
public final class StoreInst extends Instruction {
	public StoreInst(Field f) {
		super(f.getType().getTypeFactory().getVoidType(), f.isStatic() ? 2 : 3);
		setOperand(0, f);
	}
	public StoreInst(Field f, Value data) {
		this(f);
		setOperand(1, data);
	}
	public StoreInst(Field f, Value data, Value instance) {
		this(f);
		setOperand(1, data);
		setOperand(2, instance);
	}

	public Field getField() {
		return (Field)getOperand(0);
	}
	public void setField(Field f) {
		setOperand(0, f);
	}
	public Value getData() {
		return getOperand(1);
	}
	public void setData(Value v) {
		setOperand(1, v);
	}
	public Value getInstance() {
		return getOperand(2);
	}
	public void setInstance(Value v) {
		setOperand(2, v);
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkElementIndex(i, 3);
		if (i == 0) {
			checkArgument(v instanceof Field);
		} else if (i == 1) {
			Field f = getField();
			checkArgument(v.getType().isSubtypeOf(f.getType().getFieldType()));
		} else if (i == 2) {
			Field f = getField();
			if (f != null) {
				checkState(f.getType() instanceof InstanceFieldType);
				checkArgument(v.getType().isSubtypeOf(((InstanceFieldType)f.getType()).getInstanceType()));
			}
		}
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		if (getField().isStatic())
			return String.format("%s: putstatic %s#%s = %s",
					getName(),
					getField().getParent().getName(), getField().getName(),
					getData().getName());
		else
			return String.format("%s: putfield %s#%s of %s = %s",
					getName(),
					getField().getParent().getName(), getField().getName(),
					getInstance().getName(),
					getData().getName());
	}
}
