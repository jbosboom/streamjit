package edu.mit.streamjit.impl.compiler.types;

import edu.mit.streamjit.impl.compiler.Module;

/**
 * The types of Values.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public abstract class Type {
	Type() {}

	public abstract Module getModule();
	public abstract TypeFactory getTypeFactory();

	/**
	 * Tests if this Type is a subtype of the given Type.
	 *
	 * The subtype relation is reflexive and transitive, but not symmetric.
	 *
	 * By default, a Type is a subtype of another iff they are the same type,
	 * but subclasses can override this.
	 * @param other the type to compare against (must not be null)
	 * @return true iff this type is a subtype of the other type
	 */
	public boolean isSubtypeOf(Type other) {
		return equals(other);
	}

	/**
	 * Returns the category of this type, the number of local variables required
	 * to store it.
	 *
	 * All regular types are category 1 types except long and double, which are
	 * category 2 types.  The null type is a category 1 type.
	 *
	 * Other types do not have a category and will throw
	 * UnsupportedOperationException.
	 * @return this type's category
	 */
	public abstract int getCategory();
}
