package edu.mit.streamjit.impl.compiler.types;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.impl.compiler.Klass;
import edu.mit.streamjit.impl.compiler.Module;
import edu.mit.streamjit.util.ReflectionUtils;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A TypeFactory makes Types on behalf of a Module.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/7/2013
 */
public final class TypeFactory implements Iterable<Type> {
	static {
		//We depend on this because we use an IdentityHashMap.
		assert ReflectionUtils.usesObjectEquality(Klass.class);
		Class<?>[] TYPE_CLASSES = {
			WrapperType.class, ArrayType.class, ReferenceType.class,
			PrimitiveType.class, VoidType.class
		};
		MethodHandles.Lookup lookup = MethodHandles.lookup();
		ImmutableList.Builder<MethodHandle> builder = ImmutableList.builder();
		for (Class<?> c : TYPE_CLASSES) {
			//Don't try to construct abstract classes.  (Doing so results in an
			//InstantiationException from inside the MethodHandle machinery with
			//no message string, which is a bit confusing.)
			assert !java.lang.reflect.Modifier.isAbstract(c.getModifiers()) : c;
			try {
				builder.add(lookup.findConstructor(c, java.lang.invoke.MethodType.methodType(void.class, Klass.class)));
			} catch (NoSuchMethodException | IllegalAccessException ex) {
				throw new AssertionError(ex);
			}
		}
		typeCtors = builder.build();
	}
	private static final ImmutableList<MethodHandle> typeCtors;
	private final Module parent;
	private final Map<Klass, ReturnType> typeMap = new IdentityHashMap<>();
	private final List<MethodType> methodTypes = new ArrayList<>();
	private final List<StaticFieldType> staticFieldTypes = new ArrayList<>();
	private final List<InstanceFieldType> instanceFieldTypes = new ArrayList<>();
	private final BasicBlockType basicBlockType;
	private final NullType nullType;
	public TypeFactory(Module parent) {
		assert ReflectionUtils.calledDirectlyFrom(Module.class);
		this.parent = checkNotNull(parent);
		this.basicBlockType = new BasicBlockType(parent);
		this.nullType = new NullType(parent);
	}

	public ReturnType getType(Klass klass) {
		ReturnType t = typeMap.get(klass);
		if (t == null) {
			t = makeType(klass);
			typeMap.put(klass, t);
		}
		return t;
	}

	public ReturnType getType(Class<?> klass) {
		return getType(parent.getKlass(klass));
	}

	public VoidType getVoidType() {
		return (VoidType)getType(parent.getKlass(void.class));
	}

	public RegularType getRegularType(Klass klass) {
		return (RegularType)getType(klass);
	}

	public RegularType getRegularType(Class<?> klass) {
		return getRegularType(parent.getKlass(klass));
	}

	public PrimitiveType getPrimitiveType(Klass klass) {
		return (PrimitiveType)getType(klass);
	}

	public PrimitiveType getPrimitiveType(Class<?> klass) {
		return getPrimitiveType(parent.getKlass(klass));
	}

	public ReferenceType getReferenceType(Klass klass) {
		return (ReferenceType)getType(klass);
	}

	public ReferenceType getReferenceType(Class<?> klass) {
		return getReferenceType(parent.getKlass(klass));
	}

	public ArrayType getArrayType(Klass klass) {
		return (ArrayType)getType(klass);
	}

	public ArrayType getArrayType(Class<?> klass) {
		return getArrayType(parent.getKlass(klass));
	}

	public WrapperType getWrapperType(Klass klass) {
		return (WrapperType)getType(klass);
	}

	public WrapperType getWrapperType(Class<?> klass) {
		return getWrapperType(parent.getKlass(klass));
	}

	public <T extends ReturnType> T getType(Klass klass, Class<T> typeClass) {
		return typeClass.cast(getType(klass));
	}

	public <T extends ReturnType> T getType(Class<?> klass, Class<T> typeClass) {
		return getType(parent.getKlass(klass), typeClass);
	}

	public NullType getNullType() {
		return nullType;
	}

	public MethodType getMethodType(ReturnType returnType, List<RegularType> parameterTypes) {
		//If this linear search gets too expensive, we can break it up by return
		//type and perhaps first parameter type.  Another strategy would be to
		//overlay a tree through all the MethodType instances, so that a lookup
		//just chases pointers from the return type roots.
		for (MethodType t : methodTypes)
			if (t.getReturnType().equals(returnType) && t.getParameterTypes().equals(parameterTypes))
				return t;
		MethodType t = new MethodType(returnType, parameterTypes);
		methodTypes.add(t);
		return t;
	}

	public MethodType getMethodType(ReturnType returnType, RegularType... parameterTypes) {
		return getMethodType(returnType, Arrays.asList(parameterTypes));
	}

	public MethodType getMethodType(Class<?> returnType, Class<?>... parameterTypes) {
		ReturnType rt = getType(returnType);
		ImmutableList.Builder<RegularType> pt = ImmutableList.builder();
		for (Class<?> c : parameterTypes)
			pt.add(getRegularType(c));
		return getMethodType(rt, pt.build());
	}

	public MethodType getMethodType(java.lang.invoke.MethodType methodType) {
		ReturnType returnType = getType(parent.getKlass(methodType.returnType()));
		List<RegularType> parameterTypes = new ArrayList<>(methodType.parameterCount());
		for (Class<?> c : methodType.parameterList())
			parameterTypes.add(getRegularType(parent.getKlass(c)));
		return getMethodType(returnType, parameterTypes);
	}

	/**
	 * Creates a MethodType from a JVM method descriptor.  The descriptor does
	 * not contain implicit this parameters (see JVMS 4.3.3), so calling
	 * MethodType.prependParameter() on the returned MethodType may be
	 * necessary.
	 * @param methodDescriptor a method descriptor
	 * @return a MethodType corresponding to the given method descriptor
	 */
	public MethodType getMethodType(String methodDescriptor) {
		//TODO: is this the right ClassLoader?
		return getMethodType(java.lang.invoke.MethodType.fromMethodDescriptorString(methodDescriptor,
				Thread.currentThread().getContextClassLoader()));
	}

	public MethodType getMethodType(java.lang.reflect.Method method) {
		ReturnType returnType = getType(parent.getKlass(method.getReturnType()));
		Class<?>[] parameterClasses = method.getParameterTypes();
		List<RegularType> parameterTypes = new ArrayList<>(parameterClasses.length);
		if (!java.lang.reflect.Modifier.isStatic(method.getModifiers()))
			parameterTypes.add(getRegularType(parent.getKlass(method.getDeclaringClass())));
		for (Class<?> c : parameterClasses)
			parameterTypes.add(getRegularType(parent.getKlass(c)));
		return getMethodType(returnType, parameterTypes);
	}

	public MethodType getMethodType(java.lang.reflect.Constructor<?> ctor) {
		ReturnType returnType = getType(parent.getKlass(ctor.getDeclaringClass()));
		Class<?>[] parameterClasses = ctor.getParameterTypes();
		List<RegularType> parameterTypes = new ArrayList<>(parameterClasses.length);
		for (Class<?> c : parameterClasses)
			parameterTypes.add(getRegularType(parent.getKlass(c)));
		return getMethodType(returnType, parameterTypes);
	}

	public StaticFieldType getFieldType(RegularType fieldType) {
		for (StaticFieldType t : staticFieldTypes)
			if (t.getFieldType().equals(fieldType))
				return t;
		StaticFieldType t = new StaticFieldType(fieldType);
		staticFieldTypes.add(t);
		return t;
	}

	public InstanceFieldType getFieldType(ReferenceType instanceType, RegularType fieldType) {
		for (InstanceFieldType t : instanceFieldTypes)
			if (t.getFieldType().equals(fieldType) && t.getInstanceType().equals(instanceType))
				return t;
		InstanceFieldType t = new InstanceFieldType(instanceType, fieldType);
		instanceFieldTypes.add(t);
		return t;
	}

	public FieldType getFieldType(java.lang.reflect.Field field) {
		RegularType fieldType = getRegularType(field.getType());
		if (java.lang.reflect.Modifier.isStatic(field.getModifiers()))
			return getFieldType(fieldType);
		else
			return getFieldType(getReferenceType(field.getDeclaringClass()), fieldType);
	}

	public BasicBlockType getBasicBlockType() {
		return basicBlockType;
	}

	/**
	 * Returns all the types created by this TypeFactory.  There are no
	 * guarantees on iteration order.  Calling methods on this TypeFactory while
	 * the iteration is in progress may result in
	 * ConcurrentModificationException.  Types present during one iteration may
	 * not be present during another, depending on this TypeFactory's caching
	 * policy.
	 * @return an iterator over Types created by this TypeFactory
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Iterator<Type> iterator() {
		return Iterables.unmodifiableIterable(Iterables.<Type>concat(
				typeMap.values(),
				methodTypes,
				staticFieldTypes,
				instanceFieldTypes,
				ImmutableList.of(basicBlockType, nullType))
				).iterator();
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
