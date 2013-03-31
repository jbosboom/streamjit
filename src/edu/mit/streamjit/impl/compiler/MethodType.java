package edu.mit.streamjit.impl.compiler;

import com.google.common.base.Joiner;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

/**
 * MethodType represents the type of a method, including the types of its
 * arguments and its return type, which must all be RegularTypes.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class MethodType extends Type {
	private final RegularType returnType;
	private final RegularType[] argumentTypes;
	private MethodType(RegularType returnType, RegularType[] argumentTypes) {
		this.returnType = returnType;
		this.argumentTypes = argumentTypes;
	}
	public static MethodType of(RegularType returnType, RegularType... argumentTypes) {
		return new MethodType(returnType, argumentTypes);
	}
	/**
	 * Creates a MethodType from a JVM method descriptor.  If the method is an
	 * instance method, the given receiverType is prepended to the argument list
	 * from the descriptor.  (The descriptor does not contain implicit this
	 * parameters; see the JVMS 4.3.3.)
	 * @param descriptor a JVM method descriptor
	 * @param receiverType the receiver type, or null if the method is static
	 * @return a MethodType
	 */
	public static MethodType fromDescriptor(String descriptor, RegularType receiverType) {
		java.lang.invoke.MethodType mt = java.lang.invoke.MethodType.fromMethodDescriptorString(descriptor, null);
		RegularType returnType = RegularType.of(mt.returnType());
		RegularType[] argumentTypes = new RegularType[mt.parameterCount() + (receiverType != null ? 1 : 0)];
		int i = 0;
		if (receiverType != null)
			argumentTypes[++i] = receiverType;
		System.arraycopy(mt.parameterArray(), 0, argumentTypes, i, mt.parameterCount());
		return MethodType.of(returnType, argumentTypes);
	}
	public RegularType getReturnType() {
		return returnType;
	}
	public int getNumArguments() {
		return argumentTypes.length;
	}
	public RegularType getArgumentType(int x) {
		return argumentTypes[x];
	}
	public Iterator<RegularType> argumentTypeIterator() {
		return Arrays.asList(argumentTypes).iterator();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final MethodType other = (MethodType)obj;
		if (!Objects.equals(this.returnType, other.returnType))
			return false;
		if (!Arrays.deepEquals(this.argumentTypes, other.argumentTypes))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 3;
		hash = 23 * hash + Objects.hashCode(this.returnType);
		hash = 23 * hash + Arrays.deepHashCode(this.argumentTypes);
		return hash;
	}

	@Override
	public String toString() {
		return returnType.toString() + '(' + Joiner.on(", ").join(argumentTypes) + ')';
	}
}
