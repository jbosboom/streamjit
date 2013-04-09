package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.Klass;
import edu.mit.streamjit.impl.compiler.Modifier;

/**
 * A RegularType is a primitive or reference type.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public abstract class RegularType extends ReturnType {
	RegularType(Klass klass) {
		super(klass);
		Class<?> backing = klass.getBackingClass();
		//A non-void primitive, or
		checkArgument((backing != null && backing.isPrimitive() && !backing.equals(void.class)) ||
				//java.lang.Object, or
				(backing != null && backing.equals(Object.class)) ||
				//something with a superclass (includes array types), or
				klass.getSuperclass() != null ||
				//an interface (which subclasses Object at the bytecode level,
				//but not according to Java reflection).
				klass.modifiers().contains(Modifier.INTERFACE),
				"not a RegularType: %s", klass);
	}
}
