package edu.mit.streamjit.impl.compiler;

import java.util.Objects;

/**
 * Represents the type of a Field.  (Fields live in memory, so this is the JVM
 * equivalent of a pointer type that must be dereferenced rather than used
 * directly.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class FieldType extends Type {
	/**
	 * The type of the object containing this field, or null if the field is
	 * static.
	 */
	private final RegularType receiverType;
	/**
	 * The type of the object stored in the field.
	 */
	private final RegularType pointeeType;
	private FieldType(RegularType receiverType, RegularType pointeeType) {
		this.receiverType = receiverType;
		this.pointeeType = pointeeType;
	}

	public static FieldType forStatic(RegularType pointeeType) {
		return new FieldType(null, pointeeType);
	}
	public static FieldType forInstance(RegularType receiverType, RegularType pointeeType) {
		return new FieldType(receiverType, pointeeType);
	}

	public boolean isStatic() {
		return receiverType == null;
	}

	/**
	 * Returns the type of the receiver object containing this field, or null
	 * if this field is static.
	 * @return the receiver type
	 */
	public RegularType getReceiverType() {
		return receiverType;
	}

	/**
	 * Returns the type of the object stored in this field.
	 * @return the pointee type
	 */
	public RegularType getPointeeType() {
		return pointeeType;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final FieldType other = (FieldType)obj;
		if (!Objects.equals(this.receiverType, other.receiverType))
			return false;
		if (!Objects.equals(this.pointeeType, other.pointeeType))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 7;
		hash = 83 * hash + Objects.hashCode(this.receiverType);
		hash = 83 * hash + Objects.hashCode(this.pointeeType);
		return hash;
	}

	@Override
	public String toString() {
		return (receiverType != null ? receiverType : "") + ".*"+pointeeType;
	}
}
