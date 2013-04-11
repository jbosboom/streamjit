package edu.mit.streamjit.impl.compiler.types;

import edu.mit.streamjit.impl.compiler.Module;

/**
 * The type of null.  While null is a subtype of every reference type, it is not
 * a subclass of ReferenceType (or even of ReturnType) as it isn't a static type
 * (i.e., it can't be used in method descriptors).
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/11/2013
 */
public final class NullType extends Type {
	private final Module parent;
	NullType(Module parent) {
		this.parent = parent;
	}
	@Override
	public Module getModule() {
		return parent;
	}
	@Override
	public TypeFactory getTypeFactory() {
		return parent.types();
	}
	@Override
	public String toString() {
		return "<nulltype>";
	}
}
