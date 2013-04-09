package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Klass;

/**
 * A primitive type.  (Note that void is not considered a primitive type, even
 * though void.class.isPrimitive() returns true.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/7/2013
 */
public final class PrimitiveType extends RegularType {
	public PrimitiveType(Klass klass) {
		super(klass);
		Class<?> backing = klass.getBackingClass();
		checkArgument(backing != null && backing.isPrimitive() && !backing.equals(void.class),
				"not a primitive type: %s", klass);
	}

	public WrapperType wrap() {
		throw new UnsupportedOperationException("TODO");
	}
}
