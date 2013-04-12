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

	/**
	 * Returns true if this type is the given type, or this type's superclass
	 * is a subtype of the given type, or one of this type's superinterfaces is
	 * a subtype of the given type.  (Note that this definition is recursive.)
	 *
	 * This definition takes care of comparing ReferenceTypes to ArrayTypes; the
	 * other direction is handled by ArrayType.isSubtypeOf.
	 * @param other
	 * @return
	 */
	@Override
	public boolean isSubtypeOf(Type other) {
		if (equals(other))
			return true;
		Klass superclass = getKlass().getSuperclass();
		if (superclass != null && getTypeFactory().getType(superclass).isSubtypeOf(other))
			return true;
		for (Klass k : getKlass().interfaces())
			if (getTypeFactory().getType(k).isSubtypeOf(other))
				return true;
		return false;
	}
}
