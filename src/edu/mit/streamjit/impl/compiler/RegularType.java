package edu.mit.streamjit.impl.compiler;

import java.util.Objects;

/**
 * A RegularType is a Type backed by a Klass.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class RegularType extends Type {
	private final Klass klass;
	protected RegularType(Klass klass) {
		this.klass = klass;
	}
	public static RegularType of(Klass klass) {
		return new RegularType(klass);
	}

	public Klass getBackingKlass() {
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
