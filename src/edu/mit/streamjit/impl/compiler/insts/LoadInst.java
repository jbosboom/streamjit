package edu.mit.streamjit.impl.compiler.insts;

import com.google.common.base.Function;
import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Field;
import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.InstanceFieldType;

/**
 * Loads a static or instance field.  (Does not load array elements.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/14/2013
 */
public final class LoadInst extends Instruction {
	public LoadInst(Field f) {
		super(checkNotNull(f).getType().getFieldType(), f.isStatic() ? 1 : 2);
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
	public Instruction clone(Function<Value, Value> operandMap) {
		if (getNumOperands() == 1)
			return new LoadInst((Field)operandMap.apply(getField()));
		else
			return new LoadInst((Field)operandMap.apply(getField()), operandMap.apply(getInstance()));
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkElementIndex(i, 2);
		if (i == 0) {
			checkArgument(v instanceof Field);
			checkArgument(((Field)v).getType().getFieldType().isSubtypeOf(getType()));
		} else if (i == 1) {
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
			return String.format("%s (%s) = getstatic %s#%s",
					getName(), getType(),
					getField().getParent().getName(), getField().getName());
		else
			return String.format("%s (%s) = getfield %s#%s from %s",
					getName(), getType(),
					getField().getParent().getName(), getField().getName(),
					getOperand(1).getName());
	}
}
