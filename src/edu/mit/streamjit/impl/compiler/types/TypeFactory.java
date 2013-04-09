package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.impl.compiler.Klass;
import edu.mit.streamjit.impl.compiler.Module;
import edu.mit.streamjit.util.ReflectionUtils;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * A TypeFactory makes Types on behalf of a Module.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/7/2013
 */
public final class TypeFactory {
	static {
		//We depend on this because we use an IdentityHashMap.
		assert ReflectionUtils.usesObjectEquality(Klass.class);
		Class<?>[] TYPE_CLASSES = {
			WrapperType.class, ArrayType.class, ReferenceType.class,
			PrimitiveType.class, RegularType.class, VoidType.class,
			ReturnType.class
		};
		MethodHandles.Lookup lookup = MethodHandles.lookup();
		ImmutableList.Builder<MethodHandle> builder = ImmutableList.builder();
		for (Class<?> c : TYPE_CLASSES)
			try {
				builder.add(lookup.findConstructor(c, java.lang.invoke.MethodType.methodType(void.class, Klass.class)));
			} catch (NoSuchMethodException | IllegalAccessException ex) {
				throw new AssertionError(ex);
			}
		typeCtors = builder.build();
	}
	private static final ImmutableList<MethodHandle> typeCtors;
	private final Module parent;
	private final Map<Klass, ReturnType> typeMap = new IdentityHashMap<>();
	public TypeFactory(Module parent) {
		assert ReflectionUtils.calledDirectlyFrom(Module.class);
		this.parent = checkNotNull(parent);
	}

	public ReturnType getType(Klass klass) {
		ReturnType t = typeMap.get(klass);
		if (t == null) {
			t = makeType(klass);
			typeMap.put(klass, t);
		}
		return t;
	}

	public VoidType getVoidType() {
		return (VoidType)getType(parent.getKlass(void.class));
	}

	public RegularType getRegularType(Klass klass) {
		return (RegularType)getType(klass);
	}

	public PrimitiveType getPrimitiveType(Klass klass) {
		return (PrimitiveType)getType(klass);
	}

	public ReferenceType getReferenceType(Klass klass) {
		return (ReferenceType)getType(klass);
	}

	public ArrayType getArrayType(Klass klass) {
		return (ArrayType)getType(klass);
	}

	public WrapperType getWrapperType(Klass klass) {
		return (WrapperType)getType(klass);
	}

	public <T extends ReturnType> T getType(Klass klass, Class<T> typeClass) {
		return typeClass.cast(getType(klass));
	}

	private ReturnType makeType(Klass klass) {
		//Rather than have logic select which type to use, we just try them in
		//inverse-topological order (most specific first).  Pretty ugly.
		for (MethodHandle h : typeCtors)
			try {
				return (ReturnType)h.invoke(klass);
			} catch (IllegalArgumentException ex) {
				continue;
			} catch (Throwable t) {
				Thread.currentThread().stop(t);
			}
		throw new AssertionError("No type for "+klass);
	}
}
