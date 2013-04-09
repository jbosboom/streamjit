package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.checkNotNull;
import edu.mit.streamjit.impl.compiler.Klass;
import edu.mit.streamjit.impl.compiler.Module;

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

	/**
	 * Returns this type's module.  All ReturnTypes implicitly belong to a Module (the Module their Klass
	 * belongs to).
	 * @return the Module this ReturnType belongs to
	 */
	protected Module getModule() {
		return getKlass().getParent();
	}

	/**
	 * Returns this type's TypeFactory.  All ReturnTypes implicitly have a
	 * TypeFactory (the TypeFactory of the Module they belong to)
	 * @return the TypeFactory of the Module this ReturnType belongs to
	 */
	protected TypeFactory getTypeFactory() {
		return getModule().types();
	}

	@Override
	public String toString() {
		return klass.getName();
	}
}
