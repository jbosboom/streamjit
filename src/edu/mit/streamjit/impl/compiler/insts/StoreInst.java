package edu.mit.streamjit.impl.compiler.insts;

import com.google.common.base.Function;
import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Field;
import edu.mit.streamjit.impl.compiler.LocalVariable;
import edu.mit.streamjit.impl.compiler.Value;
import edu.mit.streamjit.impl.compiler.types.FieldType;
import edu.mit.streamjit.impl.compiler.types.InstanceFieldType;
import edu.mit.streamjit.impl.compiler.types.RegularType;

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
	public StoreInst(LocalVariable v, Value data) {
		super(v.getType().getTypeFactory().getVoidType(), 2);
		setOperand(0, v);
		setOperand(1, data);
	}

	public Value getLocation() {
		return getOperand(0);
	}
	public void setLocation(Value f) {
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
	public StoreInst clone(Function<Value, Value> operandMap) {
		Value newLocation = operandMap.apply(getLocation());
		if (getNumOperands() == 2)
			if (newLocation instanceof LocalVariable)
				return new StoreInst((LocalVariable)newLocation, operandMap.apply(getData()));
			else
				return new StoreInst((Field)newLocation, operandMap.apply(getData()));
		return new StoreInst((Field)newLocation, operandMap.apply(getData()), operandMap.apply(getInstance()));
	}

	@Override
	protected void checkOperand(int i, Value v) {
		checkElementIndex(i, 3);
		if (i == 0) {
			checkArgument(v instanceof Field);
		} else if (i == 1) {
			RegularType type = ((FieldType)getLocation().getType()).getFieldType();
			checkArgument(v.getType().isSubtypeOf(type));
		} else if (i == 2) {
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
		if (getLocation() instanceof LocalVariable)
			return String.format("%s: store %s = %s",
						getName(),
						getLocation().getName(),
						getData().getName());
		else {
			Field f = (Field)getLocation();
			if (f.isStatic())
				return String.format("%s: putstatic %s#%s = %s",
						getName(),
						f.getParent().getName(), f.getName(),
						getData().getName());
			else
				return String.format("%s: putfield %s#%s of %s = %s",
						getName(),
						f.getParent().getName(), f.getName(),
						getInstance().getName(),
						getData().getName());
		}
	}
}
