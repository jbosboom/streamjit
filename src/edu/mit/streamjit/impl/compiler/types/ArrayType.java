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
}
