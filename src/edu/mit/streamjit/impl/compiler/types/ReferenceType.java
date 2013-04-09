package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.checkArgument;
import edu.mit.streamjit.impl.compiler.Klass;
import edu.mit.streamjit.impl.compiler.Modifier;

/**
 * A reference type, including array types.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/7/2013
 */
public class ReferenceType extends RegularType {
	public ReferenceType(Klass klass) {
		super(klass);
		//java.lang.Object, or
		checkArgument(Object.class.equals(klass.getBackingClass()) ||
				//something with a superclass (includes array types), or
				klass.getSuperclass() != null ||
				//an interface (which subclasses Object at the bytecode level,
				//but not according to Java reflection).
				klass.modifiers().contains(Modifier.INTERFACE),
				"not a ReferenceType: %s", klass);
	}
}
