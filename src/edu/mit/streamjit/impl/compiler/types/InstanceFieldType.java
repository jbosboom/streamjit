package edu.mit.streamjit.impl.compiler.types;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/14/2013
 */
public class InstanceFieldType extends FieldType {
	private final ReferenceType instanceType;
	InstanceFieldType(ReferenceType instanceType, RegularType fieldType) {
		super(fieldType);
		this.instanceType = instanceType;
	}

	public ReferenceType getInstanceType() {
		return instanceType;
	}

	@Override
	public boolean isSubtypeOf(Type other) {
		return other instanceof InstanceFieldType &&
				getFieldType().isSubtypeOf(((InstanceFieldType)other).getFieldType()) &&
				getInstanceType().isSubtypeOf(((InstanceFieldType)other).getInstanceType());
	}

	@Override
	public String toString() {
		return String.format("%s::%s*", getInstanceType(), getFieldType());
	}
}
