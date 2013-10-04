package edu.mit.streamjit.impl.compiler;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.util.ReflectionUtils;
import edu.mit.streamjit.util.bytecode.BasicBlock;
import edu.mit.streamjit.util.bytecode.Klass;
import edu.mit.streamjit.util.bytecode.Method;
import edu.mit.streamjit.util.bytecode.Module;
import edu.mit.streamjit.util.bytecode.User;
import edu.mit.streamjit.util.bytecode.Value;
import edu.mit.streamjit.util.bytecode.insts.CallInst;
import edu.mit.streamjit.util.bytecode.insts.CastInst;
import edu.mit.streamjit.util.bytecode.insts.Instruction;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains information about a Worker subclass, detached from any particular
 * instance.  ActorArchetype's primary purpose is to hold the information
 * necessary to create common work functions for a particular worker class, with
 * method arguments for its channels and fields.  These methods may later be
 * specialized if desired through the usual inlining mechanisms.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 9/20/2013
 */
public class ActorArchetype {
	private final Class<? extends Worker<?, ?>> workerClass;
	/**
	 * The worker's nonstatic fields, final and nonfinal, including inherited
	 * fields up to but not including Filter, Splitter or Joiner (i.e., only
	 * user code fields).
	 *
	 * These are the fields that must be passed to a work method instance.
	 */
	private final ImmutableList<Field> fields;
	/**
	 * The Klass corresponding to the worker class.
	 */
	private final Klass klass;
	public ActorArchetype(Class<? extends Worker<?, ?>> workerClass, Module module) {
		this.workerClass = workerClass;
		ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
		for (Class<?> c = this.workerClass; c != Filter.class && c != Splitter.class && c != Joiner.class; c = c.getSuperclass()) {
			for (Field f : c.getDeclaredFields())
				if (!Modifier.isStatic(f.getModifiers()))
					fieldsBuilder.add(f);
		}
		this.fields = fieldsBuilder.build();
		this.klass = module.getKlass(workerClass);
	}

	public Class<? extends Worker<?, ?>> workerClass() {
		return workerClass;
	}

	public ImmutableList<Field> fields() {
		return fields;
	}

	/**
	 * Returns an upper bound on the input type of the Worker class. For
	 * example, a Filter<Integer, String> will return Integer, and a Filter<T,
	 * T> will return Object.
	 * @return an upper bound on the input type of the Worker class
	 */
	public Class<?> inputType() {
		ParameterizedType t = (ParameterizedType)TypeToken.of(workerClass).getSupertype(StreamElement.class).getType();
		return TypeToken.of(t.getActualTypeArguments()[0]).getRawType();
	}

	/**
	 * Returns an upper bound on the output type of the Worker class. For
	 * example, a Filter<Integer, String> will return String, and a Filter<T, T>
	 * will return Object.
	 * @return an upper bound on the output type of the Worker class
	 */
	public Class<?> outputType() {
		ParameterizedType t = (ParameterizedType)TypeToken.of(workerClass).getSupertype(StreamElement.class).getType();
		return TypeToken.of(t.getActualTypeArguments()[1]).getRawType();
	}

	public boolean isStateful() {
		return ReflectionUtils.getAllSupertypes(workerClass()).contains(StatefulFilter.class);
	}

	public boolean canUnboxInput() {
		if (!Primitives.isWrapperType(inputType())) return false;
		Class<?> primitiveType = Primitives.unwrap(inputType());

		Module module = klass.getParent();
		Klass filterKlass = module.getKlass(Filter.class);
		Klass splitterKlass = module.getKlass(Splitter.class);
		Klass joinerKlass = module.getKlass(Joiner.class);
		Method peek1Filter = filterKlass.getMethod("peek", module.types().getMethodType(Object.class, Filter.class, int.class));
		assert peek1Filter != null;
		Method peek1Splitter = splitterKlass.getMethod("peek", module.types().getMethodType(Object.class, Splitter.class, int.class));
		assert peek1Splitter != null;
		Method pop1Filter = filterKlass.getMethod("pop", module.types().getMethodType(Object.class, Filter.class));
		assert pop1Filter != null;
		Method pop1Splitter = splitterKlass.getMethod("pop", module.types().getMethodType(Object.class, Splitter.class));
		assert pop1Splitter != null;
		Method peek2 = joinerKlass.getMethod("peek", module.types().getMethodType(Object.class, Joiner.class, int.class, int.class));
		assert peek2 != null;
		Method pop2 = joinerKlass.getMethod("pop", module.types().getMethodType(Object.class, Joiner.class, int.class));
		assert pop2 != null;
		ImmutableSet<Method> inputMethods = ImmutableSet.of(peek1Filter, peek1Splitter, pop1Filter, pop1Splitter, peek2, pop2);

		Method work = klass.getMethodByVirtual("work", module.types().getMethodType(void.class, workerClass()));
		work.resolve();
		List<Value> inputs = new ArrayList<>();
		for (BasicBlock block : work.basicBlocks())
			for (Instruction inst : block.instructions())
				if (inst instanceof CallInst &&
						inputMethods.contains(((CallInst)inst).getMethod())) {
					inputs.add(inst);
				}

		//The only use of the value should be a CastInst to the input type,
		//followed by the proper fooValue() call.  TODO: tolerate more uses?
		Method fooValue = module.getKlass(inputType()).getMethod(primitiveType.getCanonicalName()+"Value", module.types().getMethodType(primitiveType, inputType()));
		assert fooValue != null;
		for (Value v : inputs)
			for (User u : v.users().elementSet()) {
				if (!(u instanceof CastInst))
					return false;
				for (User t : u.users().elementSet()) {
					if (!(t instanceof CallInst))
						return false;
					if (((CallInst)t).getMethod() != fooValue)
						return false;
				}
			}
		return true;
	}

	public boolean canUnboxOutput() {
		if (!Primitives.isWrapperType(outputType())) return false;
		Class<?> primitiveType = Primitives.unwrap(inputType());

		Module module = klass.getParent();
		Klass filterKlass = module.getKlass(Filter.class);
		Klass splitterKlass = module.getKlass(Splitter.class);
		Klass joinerKlass = module.getKlass(Joiner.class);
		Method push1Filter = filterKlass.getMethod("push", module.types().getMethodType(void.class, Filter.class, Object.class));
		assert push1Filter != null;
		Method push1Joiner = joinerKlass.getMethod("push", module.types().getMethodType(void.class, Joiner.class, Object.class));
		assert push1Joiner != null;
		Method push2 = splitterKlass.getMethod("push", module.types().getMethodType(void.class, Splitter.class, int.class, Object.class));
		assert push2 != null;

		Method work = klass.getMethodByVirtual("work", module.types().getMethodType(void.class, workerClass()));
		work.resolve();
		List<Value> outputs = new ArrayList<>();
		for (BasicBlock block : work.basicBlocks())
			for (CallInst inst : FluentIterable.from(block.instructions()).filter(CallInst.class))
				if (inst.getMethod().equals(push1Filter) || inst.getMethod().equals(push1Joiner))
					outputs.add(inst.getArgument(1));
				else if (inst.getMethod().equals(push2))
					outputs.add(inst.getArgument(2));

		//The pushed value must come from a CallInst to the wrapper type's
		//valueOf() method.
		//TODO: support push(peek(i)).  push(peek(i).intValue() already works)
		Method valueOf = module.getKlass(inputType()).getMethod("valueOf", module.types().getMethodType(inputType(), primitiveType));
		assert valueOf != null;
		for (Value v : outputs)
			if (!(v instanceof CallInst) || ((CallInst)v).getMethod() != valueOf)
				return false;
		return true;
	}
}
