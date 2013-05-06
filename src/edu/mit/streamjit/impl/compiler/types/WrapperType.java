package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.*;
import com.google.common.primitives.Primitives;
import edu.mit.streamjit.impl.compiler.Klass;

/**
 * A wrapper type.  (Note that java.lang.Void is not considered a wrapper type
 * because void is not considered a primitive type.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/9/2013
 */
public final class WrapperType extends ReferenceType {
	WrapperType(Klass klass) {
		super(klass);
		Class<?> backer = klass.getBackingClass();
		checkArgument(backer != null && Primitives.isWrapperType(backer) && !backer.equals(Void.class),
				"not a wrapper type: %s", klass);
	}

	public PrimitiveType unwrap() {
		//Wrapper types are always backed by Classes.
		return getTypeFactory().getPrimitiveType(getModule().getKlass(Primitives.unwrap(getKlass().getBackingClass())));
	}
}
