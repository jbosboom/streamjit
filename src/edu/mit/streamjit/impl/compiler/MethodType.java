package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * MethodType represents the type of a method, including the types of its
 * parameters and its return type, which must all be RegularTypes.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class MethodType extends Type {
	private final RegularType returnType;
	private final ImmutableList<RegularType> parameterTypes;
	private MethodType(RegularType returnType, ImmutableList<RegularType> parameterTypes) {
		this.returnType = checkNotNull(returnType);
		this.parameterTypes = parameterTypes;
	}
	public static MethodType of(RegularType returnType, RegularType... parameterTypes) {
		return new MethodType(returnType, ImmutableList.copyOf(parameterTypes));
	}
	public static MethodType of(RegularType returnType, List<RegularType> parameterTypes) {
		return new MethodType(returnType, ImmutableList.copyOf(parameterTypes));
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
		ImmutableList.Builder<RegularType> argumentTypes = ImmutableList.builder();
		if (receiverType != null)
			argumentTypes.add(receiverType);
		for (Class<?> c : mt.parameterList())
			argumentTypes.add(RegularType.of(c));
		return MethodType.of(returnType, argumentTypes.build());
	}

	public RegularType getReturnType() {
		return returnType;
	}

	public ImmutableList<RegularType> getParameterTypes() {
		return parameterTypes;
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
		if (!Objects.equals(this.parameterTypes, other.parameterTypes))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 3;
		hash = 23 * hash + Objects.hashCode(this.returnType);
		hash = 23 * hash + Objects.hashCode(this.parameterTypes);
		return hash;
	}

	@Override
	public String toString() {
		return returnType.toString() + '(' + Joiner.on(", ").join(parameterTypes) + ')';
	}
}
