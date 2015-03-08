/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.util.Pair;
import edu.mit.streamjit.util.ReflectionUtils;
import edu.mit.streamjit.util.bytecode.Access;
import edu.mit.streamjit.util.bytecode.Argument;
import edu.mit.streamjit.util.bytecode.BasicBlock;
import edu.mit.streamjit.util.bytecode.Cloning;
import edu.mit.streamjit.util.bytecode.DeadCodeElimination;
import edu.mit.streamjit.util.bytecode.Field;
import edu.mit.streamjit.util.bytecode.Klass;
import edu.mit.streamjit.util.bytecode.LocalVariable;
import edu.mit.streamjit.util.bytecode.Method;
import edu.mit.streamjit.util.bytecode.Modifier;
import edu.mit.streamjit.util.bytecode.Module;
import edu.mit.streamjit.util.bytecode.ModuleClassLoader;
import edu.mit.streamjit.util.bytecode.Value;
import edu.mit.streamjit.util.bytecode.insts.ArrayLengthInst;
import edu.mit.streamjit.util.bytecode.insts.ArrayLoadInst;
import edu.mit.streamjit.util.bytecode.insts.ArrayStoreInst;
import edu.mit.streamjit.util.bytecode.insts.BinaryInst;
import edu.mit.streamjit.util.bytecode.insts.CallInst;
import edu.mit.streamjit.util.bytecode.insts.CastInst;
import edu.mit.streamjit.util.bytecode.insts.Instruction;
import edu.mit.streamjit.util.bytecode.insts.JumpInst;
import edu.mit.streamjit.util.bytecode.insts.LoadInst;
import edu.mit.streamjit.util.bytecode.insts.ReturnInst;
import edu.mit.streamjit.util.bytecode.insts.StoreInst;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findConstructor;
import edu.mit.streamjit.util.bytecode.types.ArrayType;
import edu.mit.streamjit.util.bytecode.types.MethodType;
import edu.mit.streamjit.util.bytecode.types.PrimitiveType;
import edu.mit.streamjit.util.bytecode.types.RegularType;
import edu.mit.streamjit.util.bytecode.types.TypeFactory;
import edu.mit.streamjit.util.bytecode.types.WrapperType;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Contains information about a Worker subclass, detached from any particular
 * instance.  ActorArchetype's primary purpose is to hold the information
 * necessary to create common work functions for a particular worker class, with
 * method arguments for its channels and fields.  These methods may later be
 * specialized if desired through the usual inlining mechanisms.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
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
	private final ImmutableList<java.lang.reflect.Field> fields;
	/**
	 * The Klass corresponding to the worker class.
	 */
	private final Klass workerKlass;
	private MethodHandle constructStateHolder;
	private ImmutableMap<Pair<Class<?>, Class<?>>, MethodHandle> workMethods;
	public ActorArchetype(Class<? extends Worker<?, ?>> workerClass, Module module) {
		this.workerClass = workerClass;
		ImmutableList.Builder<java.lang.reflect.Field> fieldsBuilder = ImmutableList.builder();
		for (Class<?> c = this.workerClass; c != Filter.class && c != Splitter.class && c != Joiner.class; c = c.getSuperclass()) {
			for (java.lang.reflect.Field f : c.getDeclaredFields())
				if (!java.lang.reflect.Modifier.isStatic(f.getModifiers())) {
					f.setAccessible(true);
					fieldsBuilder.add(f);
				}
		}
		this.fields = fieldsBuilder.build();
		this.workerKlass = module.getKlass(workerClass);
	}

	public Class<? extends Worker<?, ?>> workerClass() {
		return workerClass;
	}

	public ImmutableList<java.lang.reflect.Field> fields() {
		return fields;
	}

	/**
	 * Returns the static input type of the worker class. For example, a
	 * Filter<Integer, String> will return Integer, and a Filter<T, T> will
	 * return T.
	 * @return the static input type of the worker class
	 */
	public TypeToken<?> declaredInputType() {
		ParameterizedType t = (ParameterizedType)TypeToken.of(workerClass).getSupertype(StreamElement.class).getType();
		return TypeToken.of(t.getActualTypeArguments()[0]);
	}

	/**
	 * Returns the static output type of the worker class. For example, a
	 * Filter<Integer, String> will return String, and a Filter<T, T> will
	 * return T.
	 * @return the static output type of the worker class
	 */
	public TypeToken<?> declaredOutputType() {
		ParameterizedType t = (ParameterizedType)TypeToken.of(workerClass).getSupertype(StreamElement.class).getType();
		return TypeToken.of(t.getActualTypeArguments()[1]);
	}

	public boolean isStateful() {
		return ReflectionUtils.getAllSupertypes(workerClass()).contains(StatefulFilter.class);
	}

	public void generateCode(String packageName, ModuleClassLoader loader, Iterable<WorkerActor> actors) {
		assert workMethods == null : "already generated code for "+this;
		//If we've removed all instances of this archetype, don't spin an empty class.
		if (Iterables.isEmpty(actors)) return;

		Module module = workerKlass.getParent();
		TypeFactory types = module.types();
		//We need to resolve work before making the state holder class so we
		//pick up its uses.
		Method oldWork = workerKlass.getMethodByVirtual("work", types.getMethodType(types.getVoidType(), types.getRegularType(workerKlass)));
		oldWork.resolve();

		Klass stateHolderKlass = makeStateHolderKlass(packageName);
		Klass archetypeKlass = new Klass(packageName + "." + workerKlass.getName()+"Archetype",
				module.getKlass(Object.class),
				ImmutableList.<Klass>of(),
				module);
		archetypeKlass.modifiers().addAll(EnumSet.of(Modifier.PUBLIC, Modifier.FINAL));

		Map<Pair<Class<?>, Class<?>>, Method> methods = new HashMap<>();
		for (WorkerActor a : actors) {
			Class<?> inputType = a.inputType().getRawType(), outputType = a.outputType().getRawType();
			Pair<Class<?>, Class<?>> key = new Pair<Class<?>, Class<?>>(inputType, outputType);
			if (methods.containsKey(key)) continue;

			//We modify rwork while remapping so we need a fresh clone.
			Method rwork = makeRwork(archetypeKlass, stateHolderKlass);
			for (BasicBlock b : rwork.basicBlocks())
				for (Instruction i : ImmutableList.copyOf(b.instructions()))
					if (i.operands().contains(rwork.arguments().get(0)) ||
							//TODO: also check for superclass fields
							i.operands().anyMatch(Predicates.<Value>in(workerKlass.fields())))
						remapEliminatingReceiver(i, inputType, outputType, stateHolderKlass);

			assert rwork.arguments().get(0).uses().isEmpty();
			Method work = new Method(makeWorkMethodName(inputType, outputType),
					rwork.getType().dropFirstArgument(),
					EnumSet.of(Modifier.PUBLIC, Modifier.STATIC), archetypeKlass);
			Map<Value, Value> vmap = new IdentityHashMap<>();
			vmap.put(rwork.arguments().get(0), null);
			for (int i = 1; i < rwork.arguments().size(); ++i)
				vmap.put(rwork.arguments().get(i), work.arguments().get(i-1));
			Cloning.cloneMethod(rwork, work, vmap);
			cleanWorkMethod(work);
			methods.put(key, work);
			rwork.eraseFromParent();
		}

		ImmutableMap.Builder<Pair<Class<?>, Class<?>>, MethodHandle> workMethodsBuilder = ImmutableMap.builder();
		try {
			Class<?> stateHolderClass = loader.loadClass(stateHolderKlass.getName());
			this.constructStateHolder = findConstructor(stateHolderClass);
			Class<?> archetypeClass = loader.loadClass(archetypeKlass.getName());
			ImmutableListMultimap<String, java.lang.reflect.Method> methodsByName
					= Multimaps.index(Arrays.asList(archetypeClass.getMethods()), java.lang.reflect.Method::getName);
			for (Pair<Class<?>, Class<?>> p : methods.keySet()) {
				String name = makeWorkMethodName(p.first, p.second);
				workMethodsBuilder.put(p, MethodHandles.publicLookup().unreflect(
						Iterables.getOnlyElement(methodsByName.get(name))));
			}
		} catch (ClassNotFoundException | IllegalAccessException ex) {
			throw new AssertionError(ex);
		}
		this.workMethods = workMethodsBuilder.build();
	}

	private Klass makeStateHolderKlass(String packageName) {
		Module module = workerKlass.getParent();
		Klass stateHolder = new Klass(packageName + "." + workerKlass.getName() + "StateHolder",
				module.getKlass(StateHolder.class),
				ImmutableList.<Klass>of(),
				module);
		stateHolder.modifiers().add(Modifier.PUBLIC);

		Map<Field, Field> workerToHolder = new HashMap<>();
		for (Klass k = workerKlass; k.getBackingClass() != Filter.class && k.getBackingClass() != Splitter.class && k.getBackingClass() != Joiner.class; k = k.getSuperclass())
			for (Field wf : k.fields()) {
				//Don't bother with unused fields.
				if (FluentIterable.from(wf.users()).filter(LoadInst.class).isEmpty()) continue;
				Field hf = new Field(wf.getType().getFieldType(), wf.getName(), wf.modifiers(), stateHolder);
				hf.setAccess(Access.PUBLIC);
				workerToHolder.put(wf, hf);
			}

		TypeFactory types = module.types();
		RegularType workerType = types.getRegularType(workerKlass);
		RegularType stateHolderType = types.getRegularType(stateHolder);
		Method init = new Method("<init>", types.getMethodType(stateHolderType, workerType), EnumSet.of(Modifier.PUBLIC), stateHolder);
		Argument holder = init.arguments().get(0);
		Argument worker = init.arguments().get(1);
		worker.setName("worker");
		BasicBlock initBlock = new BasicBlock(module);
		init.basicBlocks().add(initBlock);
		Method superCtor = module.getKlass(StateHolder.class).getMethods("<init>").iterator().next();
		initBlock.instructions().add(new CallInst(superCtor, worker));

		Method clinit = new Method("<clinit>", types.getMethodType(void.class), EnumSet.of(Modifier.PUBLIC, Modifier.STATIC), stateHolder);
		BasicBlock clinitBlock = new BasicBlock(module);
		clinit.basicBlocks().add(clinitBlock);

		for (Map.Entry<Field, Field> e : workerToHolder.entrySet()) {
			Field wf = e.getKey(), hf = e.getValue();
			(hf.isStatic() ? clinitBlock : initBlock).instructions().addAll(initStateHolderField(wf, hf, worker, holder));
		}

		initBlock.instructions().add(new ReturnInst(types.getVoidType()));
		clinitBlock.instructions().add(new ReturnInst(types.getVoidType()));
//		stateHolder.dump(System.out);
		return stateHolder;
	}

	private ImmutableList<Instruction> initStateHolderField(Field workerField, Field holderField, Argument worker, Argument holder) {
		assert !holderField.isStatic() || workerField.isStatic() : "can't initialize static holder field from instance worker field (no worker object available)";
		Module module = workerKlass.getParent();
		TypeFactory types = module.types();
		//We need to generate field initializers, but some worker fields may be
		//private (thus inaccessible).  Unfortunately this means we need to
		//generate reflective code.  (MethodHandles might be easier to use (just
		//call into LookupUtils), but we won't have the appropriate Lookup(s).)
		ImmutableList.Builder<Instruction> builder = ImmutableList.builder();
		CallInst field;
		if (holderField.isStatic()) {
			//Class constants are subject to access checking, but
			//Class.forName() is not.
			Method forName = module.getKlass(Class.class).getMethod("forName", types.getMethodType(Class.class, String.class));
			CallInst klass = new CallInst(forName, module.constants().getConstant(workerClass.getName()));
			builder.add(klass);
			Method getFieldByName = module.getKlass(ReflectionUtils.class)
					.getMethod("getFieldByName", types.getMethodType(java.lang.reflect.Field.class, Class.class, String.class));
			field = new CallInst(getFieldByName, klass, module.constants().getConstant(workerField.getName()));
		} else {
			Method getFieldByName = module.getKlass(ReflectionUtils.class)
					.getMethod("getFieldByName", types.getMethodType(java.lang.reflect.Field.class, Object.class, String.class));
			field = new CallInst(getFieldByName, worker, module.constants().getConstant(workerField.getName()));
		}

		Method setAccessible = module.getKlass(AccessibleObject.class).getMethod("setAccessible", types.getMethodType(void.class, AccessibleObject.class, boolean.class));
		CallInst access = new CallInst(setAccessible, field, module.constants().getConstant(true));
		String getSuffix = "";
		if (workerField.getType().getFieldType() instanceof PrimitiveType) {
			String typeName = workerField.getType().getFieldType().getKlass().getName();
			//Uppercase first character.
			getSuffix = Character.toUpperCase(typeName.charAt(0)) + typeName.substring(1);
		}
		Method getMethod = Iterables.getOnlyElement(module.getKlass(java.lang.reflect.Field.class).getMethods("get"+getSuffix));
		CallInst get = new CallInst(getMethod, field, holderField.isStatic() ? module.constants().getNullConstant() : worker);
		CastInst cast = new CastInst(holderField.getType().getFieldType(), get);
		StoreInst store = holderField.isStatic() ? new StoreInst(holderField, cast) : new StoreInst(holderField, cast, holder);
		return builder.add(field, access, get, cast, store).build();
	}

	private static String makeWorkMethodName(Class<?> inputType, Class<?> outputType) {
		return "work"+inputType.getSimpleName()+outputType.getSimpleName();
	}

	/**
	 * Creates a clone of the original work method with additional arguments but
	 * retaining a dummy receiver argument (the 'r' stands for "receiver").
	 * @return the rwork method
	 */
	private Method makeRwork(Klass archetypeKlass, Klass stateHolderKlass) {
		Module module = archetypeKlass.getParent();
		TypeFactory types = module.types();
		Method oldWork = workerKlass.getMethodByVirtual("work", types.getMethodType(types.getVoidType(), types.getRegularType(workerKlass)));

		ImmutableList.Builder<RegularType> workMethodTypeBuilder = ImmutableList.builder();
		ImmutableList.Builder<String> workMethodArgumentNameBuilder = ImmutableList.builder();

		workMethodArgumentNameBuilder.add("$stateHolder");
		workMethodTypeBuilder.add(types.getRegularType(stateHolderKlass));

		workMethodArgumentNameBuilder.add("$readInput");
		workMethodTypeBuilder.add(types.getRegularType(MethodHandle.class));
		workMethodArgumentNameBuilder.add("$writeOutput");
		workMethodTypeBuilder.add(types.getRegularType(MethodHandle.class));

		workMethodArgumentNameBuilder.add("$initialReadIndex");
		if (Joiner.class.isAssignableFrom(workerClass())) {
			//The length doubles as the inputs() rewrite.
			workMethodTypeBuilder.add(types.getRegularType(int[].class));
		} else
			workMethodTypeBuilder.add(types.getRegularType(int.class));
		workMethodArgumentNameBuilder.add("$initialWriteIndex");
		if (Splitter.class.isAssignableFrom(workerClass())) {
			//The length doubles as the outputs() rewrite.
			workMethodTypeBuilder.add(types.getRegularType(int[].class));
		} else
			workMethodTypeBuilder.add(types.getRegularType(int.class));

		MethodType workMethodType = types.getMethodType(types.getVoidType(), workMethodTypeBuilder.build());
		ImmutableList<String> workMethodArgumentNames = workMethodArgumentNameBuilder.build();

		MethodType rworkMethodType = workMethodType.prependArgument(types.getRegularType(workerClass()));
		Method rwork = new Method("rwork",
				rworkMethodType, EnumSet.of(Modifier.PRIVATE, Modifier.STATIC), archetypeKlass);
		rwork.arguments().get(0).setName("dummyReceiver");
		for (int i = 1; i < rwork.arguments().size(); ++i)
			rwork.arguments().get(i).setName(workMethodArgumentNames.get(i-1));
		Map<Value, Value> vmap = new IdentityHashMap<>();
		vmap.put(oldWork.arguments().get(0), rwork.arguments().get(0));
		Cloning.cloneMethod(oldWork, rwork, vmap);

		BasicBlock entryBlock = new BasicBlock(module, "entry");
		rwork.basicBlocks().add(0, entryBlock);
		makeArgTempCopy(rwork.getArgument("$initialReadIndex"), "readIndex", entryBlock);
		makeArgTempCopy(rwork.getArgument("$initialWriteIndex"), "writeIndex", entryBlock);
		entryBlock.instructions().add(new JumpInst(rwork.basicBlocks().get(1)));
		return rwork;
	}

	/**
	 * Set up local variables for the read and write indices (aka
	 * push/popCount). If they're arrays, we need to make a copy for this
	 * iteration to modify; we do it in the bytecode rather than the
	 * MethodHandle chain in the hopes the JVM can scalar-replace (though the
	 * array's size can vary (in general), so perhaps it never will?  we'd need
	 * to treat inputs()/outputs() as things to be specialized in ActorGroup)
	 */
	private void makeArgTempCopy(Argument arg, String localVarName, BasicBlock block) {
		Module module = workerKlass.getParent();
		LocalVariable var = new LocalVariable(arg.getType(), localVarName, block.getParent());
		Value toStore = arg;

		//Actually, we don't copy; we now depend on updates being preserved
		//until the next iteration.  See ActorGroup.specialize().
//		if (arg.getType() instanceof ArrayType) {
//			ArrayLengthInst ali = new ArrayLengthInst(arg);
//			block.instructions().add(ali);
//			Method copyOf = module.getKlass(Arrays.class).getMethod("copyOf", module.types().getMethodType(arg.getType(), arg.getType(), module.types().getRegularType(int.class)));
//			Instruction copyOfCall = new CallInst(copyOf, arg, ali);
//			block.instructions().add(copyOfCall);
//			toStore = copyOfCall;
//		}

		StoreInst store = new StoreInst(var, toStore);
		block.instructions().add(store);
	}

	private void remapEliminatingReceiver(Instruction inst, Class<?> inputType, Class<?> outputType, Klass stateHolderKlass) {
		if (inst instanceof CallInst)
			remap((CallInst)inst, inputType, outputType);
		else if (inst instanceof LoadInst)
			remap((LoadInst)inst, stateHolderKlass);
		else if (inst instanceof StoreInst)
			remap((StoreInst)inst, stateHolderKlass);
		else
			throw new AssertionError("Can't eliminiate receiver: "+inst);
	}

	private void remap(CallInst inst, Class<?> inputType, Class<?> outputType) {
		Module module = workerKlass.getParent();
		Method rwork = inst.getParent().getParent();
		Method method = inst.getMethod();
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
		Method push1Filter = filterKlass.getMethod("push", module.types().getMethodType(void.class, Filter.class, Object.class));
		assert push1Filter != null;
		Method push1Joiner = joinerKlass.getMethod("push", module.types().getMethodType(void.class, Joiner.class, Object.class));
		assert push1Joiner != null;
		Method peek2 = joinerKlass.getMethod("peek", module.types().getMethodType(Object.class, Joiner.class, int.class, int.class));
		assert peek2 != null;
		Method pop2 = joinerKlass.getMethod("pop", module.types().getMethodType(Object.class, Joiner.class, int.class));
		assert pop2 != null;
		Method push2 = splitterKlass.getMethod("push", module.types().getMethodType(void.class, Splitter.class, int.class, Object.class));
		assert push2 != null;
		Method inputs = joinerKlass.getMethod("inputs", module.types().getMethodType(int.class, Joiner.class));
		assert inputs != null;
		Method outputs = splitterKlass.getMethod("outputs", module.types().getMethodType(int.class, Splitter.class));
		assert outputs != null;
		Method invokeExact = Iterables.getOnlyElement(module.getKlass(MethodHandle.class).getMethods("invokeExact"));

		List<Instruction> insts = new ArrayList<>();
		Value replacement;
		if (method.equals(peek1Filter) || method.equals(peek1Splitter)) {
			Value peekIndex = inst.getArgument(1);
			replacement = read(null, peekIndex, inputType, rwork, insts);
		} else if (method.equals(pop1Filter) || method.equals(pop1Splitter)) {
			replacement = read(null, null, inputType, rwork, insts);
			incrementReadIndex(null, rwork, insts);
		} else if (method.equals(push1Filter) || method.equals(push1Joiner)) {
			Value item = inst.getArgument(1);
			replacement = write(null, item, outputType, rwork, insts);
		} else if (method.equals(peek2)) {
			Value channelIndex = inst.getArgument(1), peekIndex = inst.getArgument(2);
			replacement = read(channelIndex, peekIndex, inputType, rwork, insts);
		} else if (method.equals(pop2)) {
			Value channelIndex = inst.getArgument(1);
			replacement = read(channelIndex, null, inputType, rwork, insts);
			incrementReadIndex(channelIndex, rwork, insts);
		} else if (method.equals(push2)) {
			Value channelIndex = inst.getArgument(1), item = inst.getArgument(2);
			replacement = write(channelIndex, item, outputType, rwork, insts);
		} else if (method.equals(inputs)) {
			LoadInst readIndex = new LoadInst(rwork.getLocalVariable("readIndex"));
			insts.add(readIndex);
			ArrayLengthInst length = new ArrayLengthInst(readIndex);
			insts.add(length);
			replacement = length;
		} else if (method.equals(outputs)) {
			LoadInst writeIndex = new LoadInst(rwork.getLocalVariable("writeIndex"));
			insts.add(writeIndex);
			ArrayLengthInst length = new ArrayLengthInst(writeIndex);
			insts.add(length);
			replacement = length;
		} else
			throw new AssertionError(inst);
		inst.replaceInstWithInsts(replacement, insts);
	}

	private Value read(Value channelIndex, Value readOffset, Class<?> inputType, Method rwork, List<Instruction> insts) {
		Module module = rwork.getParent().getParent();
		Method invokeExact = Iterables.getOnlyElement(module.getKlass(MethodHandle.class).getMethods("invokeExact"));

		Value readIndex = getReadIndex(channelIndex, rwork, insts);
		if (readOffset != null) {
			BinaryInst actualIndex = new BinaryInst(readIndex, BinaryInst.Operation.ADD, readOffset);
			actualIndex.setName("readIndex_plus_peek");
			insts.add(actualIndex);
			readIndex = actualIndex;
		}

		Argument readHandle = rwork.getArgument("$readInput");
		CallInst invoke;
		if (channelIndex == null)
			invoke = new CallInst(invokeExact, module.types().getMethodType(inputType, MethodHandle.class, int.class), readHandle, readIndex);
		else
			invoke = new CallInst(invokeExact, module.types().getMethodType(inputType, MethodHandle.class, int.class, int.class), readHandle, channelIndex, readIndex);
		insts.add(invoke);
		invoke.setName("readItem");

		Value value = invoke;
		if (inputType.isPrimitive())
			value = box(value, insts);
		return value;
	}

	private Value write(Value channelIndex, Value item, Class<?> outputType, Method rwork, List<Instruction> insts) {
		Module module = rwork.getParent().getParent();
		Method invokeExact = Iterables.getOnlyElement(module.getKlass(MethodHandle.class).getMethods("invokeExact"));

		Value writeIndex = getWriteIndex(channelIndex, rwork, insts);

		if (outputType.isPrimitive())
			item = unbox(item, outputType, insts);
		else if (!item.getType().isSubtypeOf(module.types().getType(outputType))) {
			CastInst cast = new CastInst(module.types().getType(outputType), item);
			insts.add(cast);
			item = cast;
		}

		Argument writeHandle = rwork.getArgument("$writeOutput");
		CallInst invoke;
		if (channelIndex == null)
			invoke = new CallInst(invokeExact, module.types().getMethodType(void.class, MethodHandle.class, int.class, outputType), writeHandle, writeIndex, item);
		else
			invoke = new CallInst(invokeExact, module.types().getMethodType(void.class, MethodHandle.class, int.class, int.class, outputType), writeHandle, channelIndex, writeIndex, item);
		insts.add(invoke);
		invoke.setName("writeItem");

		incrementWriteIndex(channelIndex, rwork, insts);
		return invoke;
	}

	/**
	 * Gets the current read index for the given channel.  Pass null for the
	 * channel index if there's only one input.
	 * @param channelIndex the channel index value, or null if only one input
	 * @param rwork the rwork method
	 * @param insts the insts being constructed
	 * @return the read index
	 */
	private Value getReadIndex(Value channelIndex, Method rwork, List<Instruction> insts) {
		return getIndex(channelIndex, "readIndex", rwork, insts);
	}

	/**
	 * Gets the current write index for the given channel.  Pass null for the
	 * channel index if there's only one output.
	 * @param channelIndex the channel index value, or null if only one output
	 * @param rwork the rwork method
	 * @param insts the insts being constructed
	 * @return the read index
	 */
	private Value getWriteIndex(Value channelIndex, Method rwork, List<Instruction> insts) {
		return getIndex(channelIndex, "writeIndex", rwork, insts);
	}

	private Value getIndex(Value channelIndex, String indexVariableName, Method rwork, List<Instruction> insts) {
		LoadInst readIndex = new LoadInst(rwork.getLocalVariable(indexVariableName));
		insts.add(readIndex);
		if (readIndex.getType() instanceof ArrayType) {
			ArrayLoadInst index = new ArrayLoadInst(readIndex, channelIndex);
			insts.add(index);
			return index;
		}
		assert channelIndex == null;
		return readIndex;
	}

	private void incrementReadIndex(Value channelIndex, Method rwork, List<Instruction> insts) {
		incrementIndex(channelIndex, "readIndex", rwork, insts);
	}

	private void incrementWriteIndex(Value channelIndex, Method rwork, List<Instruction> insts) {
		incrementIndex(channelIndex, "writeIndex", rwork, insts);
	}

	private void incrementIndex(Value channelIndex, String indexVariableName, Method rwork, List<Instruction> insts) {
		LocalVariable localVar = rwork.getLocalVariable(indexVariableName);
		LoadInst readIndex = new LoadInst(localVar);
		insts.add(readIndex);
		Value index;
		if (readIndex.getType() instanceof ArrayType) {
			ArrayLoadInst load = new ArrayLoadInst(readIndex, channelIndex);
			insts.add(load);
			index = load;
		} else {
			assert channelIndex == null;
			index = readIndex;
		}

		BinaryInst addOne = new BinaryInst(index, BinaryInst.Operation.ADD, rwork.getParent().getParent().constants().getConstant(1));
		insts.add(addOne);

		Instruction store;
		if (readIndex.getType() instanceof ArrayType) {
			store = new ArrayStoreInst(readIndex, channelIndex, addOne);
		} else {
			store = new StoreInst(localVar, addOne);
		}
		insts.add(store);
	}

	private Value box(Value v, List<Instruction> insts) {
		assert v.getType() instanceof PrimitiveType : "boxing nonprimitive "+v;
		PrimitiveType prim = (PrimitiveType)v.getType();
		WrapperType wrapper = prim.wrap();
		Method valueOf = wrapper.getKlass().getMethod("valueOf", wrapper.getTypeFactory().getMethodType(wrapper, prim));
		CallInst ci = new CallInst(valueOf, v);
		insts.add(ci);
		ci.setName(v.getName()+"_boxed");
		return ci;
	}

	//We need targetType in case the value is just an Object.
	private Value unbox(Value v, Class<?> targetType, List<Instruction> insts) {
		TypeFactory types = v.getType().getTypeFactory();

		assert v.getType() instanceof WrapperType || v.getType().equals(types.getType(Object.class)) : "unboxing bad type "+v;
		PrimitiveType prim = types.getPrimitiveType(targetType);
		WrapperType wrapper = prim.wrap();
		String name = v.getName();
		if (v.getType().equals(types.getType(Object.class))) {
			CastInst ci = new CastInst(wrapper, v);
			insts.add(ci);
			v = ci;
		}

		Method fooValue = wrapper.getKlass().getMethod(prim.toString()+"Value", types.getMethodType(prim, wrapper));
		CallInst call = new CallInst(fooValue, v);
		insts.add(call);
		call.setName(name+"_unboxed");
		return call;
	}

	private void remap(LoadInst inst, Klass stateHolderKlass) {
		assert inst.getLocation() instanceof Field;
		Argument stateHolder = inst.getParent().getParent().getArgument("$stateHolder");
		Field stateField = stateHolderKlass.getField(inst.getLocation().getName());
		assert stateField != null : inst;
		inst.replaceInstWithInst(stateField.isStatic() ? new LoadInst(stateField) : new LoadInst(stateField, stateHolder));
	}

	private void remap(StoreInst inst, Klass stateHolderKlass) {
		assert inst.getLocation() instanceof Field;
		Argument stateHolder = inst.getParent().getParent().getArgument("$stateHolder");
		Field stateField = stateHolderKlass.getField(inst.getLocation().getName());
		assert stateField != null : inst;
		inst.replaceInstWithInst(new StoreInst(stateField, inst.getData(), stateHolder));
	}

	/**
	 * Cleans up a work method by removing dead code, including code that isn't
	 * in general dead but is dead in our specific cases.
	 * @param work the work method
	 */
	private void cleanWorkMethod(Method work) {
		boolean progress;
		do {
			progress = false;
			progress |= DeadCodeElimination.eliminateDeadCode(work);
			progress |= removeUnusedReads(work);
		} while (progress);
	}

	/**
	 * Removes reads that aren't used by anything.  Peeking filters tend to
	 * compute a result via peeking and then pop some of their inputs rather
	 * than mix peeks and pops; we need only increment the read index for those
	 * unused pops.  However, we can't easily tell if the pop is used when
	 * remapping, as it will often be used by a generics-induced cast to the
	 * original input type, so we wait until after running standard DCE.
	 * @param work the work method to remove reads from
	 * @return true iff changes were made
	 */
	private boolean removeUnusedReads(Method work) {
		Argument readHandle = work.getArgument("$readInput_clone");
		for (BasicBlock block : work.basicBlocks())
			for (Instruction i : block.instructions()) {
				if (!(i instanceof CallInst)) continue;
				if (!i.uses().isEmpty()) continue;
				CallInst ci = (CallInst)i;
				//Nothing else uses the read handle, so this is sufficient.
				if (ci.getArgument(0).equals(readHandle)) {
					ci.eraseFromParent();
					return true;
				}
			}
		return false;
	}

	public StateHolder makeStateHolder(WorkerActor a) {
		checkArgument(a.archetype() == this);
		try {
			return (StateHolder)constructStateHolder.invoke(a.worker());
		} catch (Throwable ex) {
			throw new AssertionError(ex);
		}
	}

	/**
	 * Specializes an archetypal work function for the given Actor.  The
	 * returned function takes four arguments: the read and write method handles
	 * and the initial read and write indices (int or int[] as appropriate).
	 * These four arguments depend on the ActorGroup iterations having been
	 * assigned to cores, so can't be bound just based on the Actor.
	 * @param a the Actor to specialize for
	 * @return a specialized work method
	 */
	public MethodHandle specialize(WorkerActor a) {
		checkArgument(a.archetype() == this);
		MethodHandle handle = workMethods.get(new Pair<>(a.inputType().getRawType(), a.outputType().getRawType()));
		return handle.bindTo(a.stateHolder());
	}
}
