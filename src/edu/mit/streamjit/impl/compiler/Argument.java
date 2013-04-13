package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.impl.compiler.types.RegularType;

/**
 * An Argument represents an argument to a Method.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class Argument extends Value implements Parented<Method> {
	private final Method parent;
	public Argument(Method parent, RegularType type) {
		super(type);
		this.parent = parent;
	}
	public Argument(Method parent, RegularType type, String name) {
		super(type, name);
		this.parent = parent;
	}

	@Override
	public Method getParent() {
		return parent;
	}

	@Override
	public RegularType getType() {
		return (RegularType)super.getType();
	}
}
