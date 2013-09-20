package edu.mit.streamjit.util.bytecode.types;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/14/2013
 */
public final class StaticFieldType extends FieldType {
	StaticFieldType(RegularType fieldType) {
		super(fieldType);
	}

	@Override
	public boolean isSubtypeOf(Type other) {
		return other instanceof StaticFieldType &&
				getFieldType().isSubtypeOf(((StaticFieldType)other).getFieldType());
	}

	@Override
	public String toString() {
		return getFieldType().toString()+"*";
	}
}
