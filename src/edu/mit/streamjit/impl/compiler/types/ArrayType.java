package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Klass;

/**
 * An array type.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/7/2013
 */
public final class ArrayType extends ReferenceType {
	ArrayType(Klass klass) {
		super(klass);
		checkArgument(klass.isArray(), "not array: %s", klass);
	}

	public int getDimensions() {
		return getKlass().getDimensions();
	}

	public RegularType getComponentType() {
		return getTypeFactory().getRegularType(getKlass().getComponentKlass());
	}

	public RegularType getElementType() {
		return getTypeFactory().getRegularType(getKlass().getElementKlass());
	}

	@Override
	public boolean isSubtypeOf(Type other) {
		if (other instanceof ArrayType) {
			RegularType ct = getComponentType();
			RegularType oct = ((ArrayType)other).getComponentType();
			if (ct instanceof PrimitiveType && oct instanceof PrimitiveType)
				return ct.equals(oct);
			else if (ct instanceof ReferenceType && oct instanceof ReferenceType)
				return ct.isSubtypeOf(oct);
			else
				return false;
		} else if (other instanceof ReferenceType) {
			Klass rtk = ((ReferenceType)other).getKlass();
			//Object, Cloneable or Serializable
			return rtk.equals(getKlass().getSuperclass()) || getKlass().interfaces().contains(rtk);
		} else
			return false;
	}
}
