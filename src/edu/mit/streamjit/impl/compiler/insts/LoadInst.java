package edu.mit.streamjit.impl.compiler.insts;

import com.google.common.base.Function;
import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Field;
import edu.mit.streamjit.impl.compiler.LocalVariable;
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
	public LoadInst(LocalVariable v) {
		super(checkNotNull(v).getType().getFieldType(), 1);
		setOperand(0, v);
	}

	public Value getLocation() {
		return getOperand(0);
	}
	public void setLocation(Value f) {
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
		Value newLocation = operandMap.apply(getLocation());
		if (getNumOperands() == 1)
			if (newLocation instanceof Field)
				return new LoadInst((Field)newLocation);
			else
				return new LoadInst((LocalVariable)newLocation);
		else
			return new LoadInst((Field)newLocation, operandMap.apply(getInstance()));
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkElementIndex(i, 2);
		if (i == 0) {
			checkArgument(v instanceof Field);
			checkArgument(((Field)v).getType().getFieldType().isSubtypeOf(getType()));
		} else if (i == 1) {
			Field f = (Field)getLocation();
			if (f != null) {
				checkState(f.getType() instanceof InstanceFieldType);
				checkArgument(v.getType().isSubtypeOf(((InstanceFieldType)f.getType()).getInstanceType()));
			}
		}
		super.checkOperand(i, v);
	}

	@Override
	public String toString() {
		if (getLocation() instanceof LocalVariable) {
			return String.format("%s (%s) = load %s",
						getName(), getType(),
						getLocation().getName());
		} else {
			Field f = (Field)getLocation();
			if (f.isStatic())
				return String.format("%s (%s) = getstatic %s#%s",
						getName(), getType(),
						f.getParent().getName(), f.getName());
			else
				return String.format("%s (%s) = getfield %s#%s from %s",
						getName(), getType(),
						f.getParent().getName(), f.getName(),
						getOperand(1).getName());
		}
	}
}
