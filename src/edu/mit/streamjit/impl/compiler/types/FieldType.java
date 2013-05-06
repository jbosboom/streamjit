package edu.mit.streamjit.impl.compiler.types;

import edu.mit.streamjit.impl.compiler.Module;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/14/2013
 */
public abstract class FieldType extends Type {
	//The type of primitive or object stored in the field.
	private final RegularType fieldType;
	FieldType(RegularType fieldType) {
		this.fieldType = fieldType;
	}
	public RegularType getFieldType() {
		return fieldType;
	}
	@Override
	public int getCategory() {
		throw new UnsupportedOperationException();
	}
	@Override
	public Module getModule() {
		return fieldType.getModule();
	}
	@Override
	public TypeFactory getTypeFactory() {
		return fieldType.getTypeFactory();
	}
}
