package edu.mit.streamjit.util.bytecode.types;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.util.bytecode.Module;
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

	public MethodType withReturnType(ReturnType newReturnType) {
		return getTypeFactory().getMethodType(newReturnType, parameterTypes);
	}

	public MethodType prependArgument(RegularType newParameterType) {
		return getTypeFactory().getMethodType(returnType,
				ImmutableList.<RegularType>builder().add(newParameterType).addAll(parameterTypes).build());
	}

	public MethodType dropFirstArgument() {
		return getTypeFactory().getMethodType(returnType, parameterTypes.subList(1, parameterTypes.size()));
	}

	public MethodType appendArgument(RegularType newParameterType) {
		return getTypeFactory().getMethodType(returnType,
				ImmutableList.<RegularType>builder().addAll(parameterTypes).add(newParameterType).build());
	}

	public MethodType dropLastArgument() {
		return getTypeFactory().getMethodType(returnType, parameterTypes.subList(0, parameterTypes.size()-1));
	}

	public String getDescriptor() {
		StringBuilder sb = new StringBuilder();
		sb.append('(');
		for (RegularType t : getParameterTypes())
			sb.append(t.getDescriptor());
		sb.append(')');
		sb.append(returnType.getDescriptor());
		return sb.toString();
	}

	@Override
	public int getCategory() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return returnType.toString() + '(' + Joiner.on(", ").join(parameterTypes) + ')';
	}

	@Override
	public Module getModule() {
		return returnType.getModule();
	}

	@Override
	public TypeFactory getTypeFactory() {
		return returnType.getTypeFactory();
	}
}
