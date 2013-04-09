package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.checkArgument;
import edu.mit.streamjit.impl.compiler.Klass;

/**
 * A reference type, including array types.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/7/2013
 */
public class ReferenceType extends RegularType {
	public ReferenceType(Klass klass) {
		super(klass);
		//A RegularType that isn't a primitive type.  Primitives all have
		//backing classes, so we can check isPrimitive(); RegularType will
		//exclude void (for which isPrimitive() returns true).
		Class<?> backing = klass.getBackingClass();
		checkArgument(backing == null || !backing.isPrimitive(),
				"not a ReferenceType: %s", klass);
	}
}
