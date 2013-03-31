package edu.mit.streamjit.impl.compiler;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * MethodType represents the type of a method, including the types of its
 * arguments and its return type, which must all be RegularTypes.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class MethodType extends Type {
	private final RegularType returnType;
	private final ImmutableList<RegularType> argumentTypes;
	private MethodType(RegularType returnType, ImmutableList<RegularType> argumentTypes) {
		this.returnType = returnType;
		this.argumentTypes = argumentTypes;
	}
	public static MethodType of(RegularType returnType, RegularType... argumentTypes) {
		return new MethodType(returnType, ImmutableList.copyOf(argumentTypes));
	}
	public static MethodType of(RegularType returnType, List<RegularType> argumentTypes) {
		return new MethodType(returnType, ImmutableList.copyOf(argumentTypes));
	}
	/**
	 * Creates a MethodType from a JVM method descriptor.  The descriptor does
	 * not contain implicit this parameters (see JVMS 4.3.3), so if the method
	 * is an instance method, the given receiver type must be provided and will
	 * be prepended to the parameters from the descriptor; if the method is
	 * static, pass null.
	 * @param descriptor a JVM method descriptor
	 * @param receiverType the receiver type, or null if the method is static
	 * @return a MethodType
	 */
	public static MethodType fromDescriptor(String descriptor, RegularType receiverType) {
		java.lang.invoke.MethodType mt = java.lang.invoke.MethodType.fromMethodDescriptorString(descriptor, null);
		RegularType returnType = RegularType.of(mt.returnType());
		ImmutableList.Builder<RegularType> builder = ImmutableList.builder();
		if (receiverType != null)
			builder.add(receiverType);
		for (Class<?> c : mt.parameterList())
			builder.add(RegularType.of(c));
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
		return argumentTypes.size();
	}
	public RegularType getArgumentType(int x) {
		return argumentTypes.get(x);
	}
	public Iterator<RegularType> argumentTypeIterator() {
		return argumentTypes.iterator();
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
		if (!Objects.equals(this.argumentTypes, other.argumentTypes))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 3;
		hash = 23 * hash + Objects.hashCode(this.returnType);
		hash = 23 * hash + Objects.hashCode(this.argumentTypes);
		return hash;
	}

	@Override
	public String toString() {
		return returnType.toString() + '(' + Joiner.on(", ").join(argumentTypes) + ')';
	}
}
