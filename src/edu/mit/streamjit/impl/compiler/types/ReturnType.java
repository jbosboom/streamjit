package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.checkNotNull;
import edu.mit.streamjit.impl.compiler.Klass;

/**
 * A ReturnType is a type that can be used as the return type of a method; that
 * is, a RegularType or void.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/7/2013
 */
public abstract class ReturnType extends Type {
	private final Klass klass;
	ReturnType(Klass klass) {
		this.klass = checkNotNull(klass);
	}

	public Klass getKlass() {
		return klass;
	}

	@Override
	public String toString() {
		return klass.getName();
	}
}
