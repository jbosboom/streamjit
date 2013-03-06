package edu.mit.streamjit.impl.compiler;

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
		StringBuilder sb = new StringBuilder();
		sb.append(returnType);
		sb.append('(');
		if (argumentTypes.length >= 1)
			sb.append(argumentTypes[0]);
		for (int i = 1; i < argumentTypes.length; ++i)
			sb.append(", ").append(argumentTypes[i]);
		sb.append(')');
		return sb.toString();
	}
}
