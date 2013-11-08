package edu.mit.streamjit.util.bytecode.types;

import static com.google.common.base.Preconditions.checkArgument;
import edu.mit.streamjit.util.bytecode.Klass;

/**
 * The void type.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/7/2013
 */
public final class VoidType extends ReturnType {
	VoidType(Klass klass) {
		super(klass);
		checkArgument(klass.getBackingClass().equals(void.class), "%s not VoidType", klass);
	}

	@Override
	public String getDescriptor() {
		return "V";
	}

	@Override
	public int getCategory() {
		throw new UnsupportedOperationException();
	}
}
