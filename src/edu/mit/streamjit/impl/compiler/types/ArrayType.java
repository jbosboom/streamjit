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
		throw new UnsupportedOperationException("TODO");
	}

	public Type getComponentType() {
		throw new UnsupportedOperationException("TODO");
	}

	public Type getElementType() {
		throw new UnsupportedOperationException("TODO");
	}
}
