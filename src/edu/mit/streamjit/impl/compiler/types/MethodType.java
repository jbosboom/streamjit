package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.List;

/**
 * MethodType represents the type of a method, including the types of its
 * parameters (which must be RegularTypes) and its return type (which must be a
 * ReturnType).
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public class MethodType extends Type {
	private final ReturnType returnType;
	private final ImmutableList<RegularType> parameterTypes;
	MethodType(ReturnType returnType, List<RegularType> parameterTypes) {
		this.returnType = checkNotNull(returnType);
		this.parameterTypes = ImmutableList.copyOf(parameterTypes);
	}

	public ReturnType getReturnType() {
		return returnType;
	}

	public ImmutableList<RegularType> getParameterTypes() {
		return parameterTypes;
	}

	@Override
	public String toString() {
		return returnType.toString() + '(' + Joiner.on(", ").join(parameterTypes) + ')';
	}
}
