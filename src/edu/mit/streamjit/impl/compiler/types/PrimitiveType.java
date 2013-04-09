package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.*;
import com.google.common.primitives.Primitives;
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
		//PrimitiveTypes are always backed by Classes.
		return getTypeFactory().getWrapperType(getModule().getKlass(Primitives.wrap(getKlass().getBackingClass())));
	}
}
