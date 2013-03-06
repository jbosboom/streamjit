package edu.mit.streamjit.impl.compiler;

import java.util.Objects;

/**
 * A RegularType is a Type backed by a Class<?> object, with forwarding methods
 * for common operations.  These represent all types that could be directly
 * represented by Class objects, including classes, interfaces, primitives,
 * array types, and the void type.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class RegularType extends Type {
	private final Class<?> klass;
	protected RegularType(Class<?> klass) {
		this.klass = klass;
	}
	/**
	 * Returns the RegularType corresponding to the given Class<?> object.
	 * @param klass the klass
	 * @return a RegularType object
	 */
	public static RegularType of(Class<?> klass) {
		return new RegularType(klass);
	}

	public Class<?> getBackingClass() {
		return klass;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final RegularType other = (RegularType)obj;
		if (!Objects.equals(this.klass, other.klass))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 3;
		hash = 17 * hash + Objects.hashCode(this.klass);
		return hash;
	}

	@Override
	public String toString() {
		return klass.toString();
	}
}
