package edu.mit.streamjit.impl.compiler2;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.util.ReflectionUtils;
import edu.mit.streamjit.util.bytecode.Argument;
import edu.mit.streamjit.util.bytecode.BasicBlock;
import edu.mit.streamjit.util.bytecode.Cloning;
import edu.mit.streamjit.util.bytecode.Field;
import edu.mit.streamjit.util.bytecode.Klass;
import edu.mit.streamjit.util.bytecode.LocalVariable;
import edu.mit.streamjit.util.bytecode.Method;
import edu.mit.streamjit.util.bytecode.Modifier;
import edu.mit.streamjit.util.bytecode.Module;
import edu.mit.streamjit.util.bytecode.ModuleClassLoader;
import edu.mit.streamjit.util.bytecode.User;
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
import edu.mit.streamjit.util.bytecode.insts.StoreInst;
import edu.mit.streamjit.util.bytecode.types.ArrayType;
import edu.mit.streamjit.util.bytecode.types.MethodType;
import edu.mit.streamjit.util.bytecode.types.RegularType;
import edu.mit.streamjit.util.bytecode.types.TypeFactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

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
	private final ImmutableList<java.lang.reflect.Field> fields;
	/**
	 * The Klass corresponding to the worker class.
	 */
	private final Klass workerKlass;
	private MethodHandle workMethod;
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

		Module module = workerKlass.getParent();
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

		Method work = workerKlass.getMethodByVirtual("work", module.types().getMethodType(void.class, workerClass()));
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

		Module module = workerKlass.getParent();
		Klass filterKlass = module.getKlass(Filter.class);
		Klass splitterKlass = module.getKlass(Splitter.class);
		Klass joinerKlass = module.getKlass(Joiner.class);
		Method push1Filter = filterKlass.getMethod("push", module.types().getMethodType(void.class, Filter.class, Object.class));
		assert push1Filter != null;
		Method push1Joiner = joinerKlass.getMethod("push", module.types().getMethodType(void.class, Joiner.class, Object.class));
		assert push1Joiner != null;
		Method push2 = splitterKlass.getMethod("push", module.types().getMethodType(void.class, Splitter.class, int.class, Object.class));
		assert push2 != null;

		Method work = workerKlass.getMethodByVirtual("work", module.types().getMethodType(void.class, workerClass()));
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

	public void generateCode(String packageName, ModuleClassLoader loader) {
		if (workMethod != null)
			return;

		Module module = workerKlass.getParent();
		Klass archetypeKlass = new Klass(packageName + "." + workerKlass.getName()+"Archetype",
				module.getKlass(Object.class),
				ImmutableList.<Klass>of(),
				module);
		archetypeKlass.modifiers().addAll(EnumSet.of(Modifier.PUBLIC, Modifier.FINAL));

		//We first make a method with a dummy receiver argument and clone the
		//original work method into it.  After remapping away any instructions
		//that use the receiver, we make the actual work method without it.
		TypeFactory types = module.types();
		Method oldWork = workerKlass.getMethodByVirtual("work", types.getMethodType(types.getVoidType(), types.getRegularType(workerKlass)));
		oldWork.resolve();

		ImmutableList.Builder<RegularType> workMethodTypeBuilder = ImmutableList.builder();
		ImmutableList.Builder<String> workMethodArgumentNameBuilder = ImmutableList.builder();

		if (StatefulFilter.class.isAssignableFrom(workerClass())) {
			//Stateful filters either get read/write MHs (which can point to the
			//worker itself, but require invoker trampolines or signature
			//polymorphism), or get a reference to a spun StateCollector class
			//which can be loaded and stored normally (or perhaps loaded once
			//into locals at entry and stored on exit, so the JVM knows only
			//used in one place).
			throw new UnsupportedOperationException("TODO");
		} else {
			//Stateless workers just get the values bound in directly.
			for (java.lang.reflect.Field f : fields) {
				workMethodArgumentNameBuilder.add(f.getName());
				workMethodTypeBuilder.add(types.getRegularType(f.getType()));
			}
		}

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

		for (BasicBlock b : rwork.basicBlocks())
			for (Instruction i : ImmutableList.copyOf(b.instructions()))
				if (Iterables.contains(i.operands(), rwork.arguments().get(0)))
					remapEliminatingReceiver(i);

		assert rwork.arguments().get(0).uses().isEmpty();
		Method work = new Method("work",
				workMethodType, EnumSet.of(Modifier.PUBLIC, Modifier.STATIC), archetypeKlass);
		vmap.clear();
		vmap.put(rwork.arguments().get(0), null);
		for (int i = 1; i < rwork.arguments().size(); ++i)
			vmap.put(rwork.arguments().get(i), work.arguments().get(i-1));
		Cloning.cloneMethod(rwork, work, vmap);
		rwork.eraseFromParent();
		try {
			Class<?> archetypeClass = loader.loadClass(archetypeKlass.getName());
			for (java.lang.reflect.Method m : archetypeClass.getMethods())
				if (m.getName().equals("work"))
					workMethod = MethodHandles.publicLookup().unreflect(m);
		} catch (ClassNotFoundException | IllegalAccessException ex) {
			throw new AssertionError(ex);
		}
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

	private void remapEliminatingReceiver(Instruction inst) {
		if (inst instanceof CallInst)
			remap((CallInst)inst);
		else if (inst instanceof LoadInst)
			remap((LoadInst)inst);
		else if (inst instanceof StoreInst)
			remap((StoreInst)inst);
		else
			throw new AssertionError("Can't eliminiate receiver: "+inst);
	}

	private void remap(CallInst inst) {
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

		//TODO: understand unboxing
		if (method.equals(peek1Filter) || method.equals(peek1Splitter)) {
			Value peekIndex = inst.getArgument(1);
			LoadInst readIndex = new LoadInst(rwork.getLocalVariable("readIndex"));
			BinaryInst actualIndex = new BinaryInst(readIndex, BinaryInst.Operation.ADD, peekIndex);
			Argument readInput = rwork.getArgument("$readInput");
			CallInst invoke = new CallInst(invokeExact, module.types().getMethodType(Object.class, MethodHandle.class, int.class), readInput, actualIndex);
			invoke.setName("peekedItem");
			inst.replaceInstWithInsts(invoke, readIndex, actualIndex, invoke);
		} else if (method.equals(pop1Filter) || method.equals(pop1Splitter)) {
			LoadInst readIndex = new LoadInst(rwork.getLocalVariable("readIndex"));
			Argument readInput = rwork.getArgument("$readInput");
			CallInst invoke = new CallInst(invokeExact, module.types().getMethodType(Object.class, MethodHandle.class, int.class), readInput, readIndex);
			invoke.setName("poppedItem");
			BinaryInst incReadIndex = new BinaryInst(readIndex, BinaryInst.Operation.ADD, module.constants().getSmallestIntConstant(1));
			StoreInst storeReadIndex = new StoreInst(rwork.getLocalVariable("readIndex"), incReadIndex);
			inst.replaceInstWithInsts(invoke, readIndex, invoke, incReadIndex, storeReadIndex);
		} else if (method.equals(push1Filter) || method.equals(push1Joiner)) {
			Value item = inst.getArgument(1);
			LoadInst writeIndex = new LoadInst(rwork.getLocalVariable("writeIndex"));
			Argument writeOutput = rwork.getArgument("$writeOutput");
			CallInst invoke = new CallInst(invokeExact, module.types().getMethodType(void.class, MethodHandle.class, int.class, Object.class), writeOutput, writeIndex, item);
			BinaryInst incWriteIndex = new BinaryInst(writeIndex, BinaryInst.Operation.ADD, module.constants().getSmallestIntConstant(1));
			StoreInst storeWriteIndex = new StoreInst(rwork.getLocalVariable("writeIndex"), incWriteIndex);
			inst.replaceInstWithInsts(invoke, writeIndex, invoke, incWriteIndex, storeWriteIndex);
		} else if (method.equals(peek2)) {
			Value channelIndex = inst.getArgument(1), peekIndex = inst.getArgument(2);
			LoadInst readIndices = new LoadInst(rwork.getLocalVariable("readIndex"));
			ArrayLoadInst readIndex = new ArrayLoadInst(readIndices, channelIndex);
			BinaryInst actualIndex = new BinaryInst(readIndex, BinaryInst.Operation.ADD, peekIndex);
			Argument readInput = rwork.getArgument("$readInput");
			CallInst invoke = new CallInst(invokeExact, module.types().getMethodType(Object.class, MethodHandle.class, int.class, int.class), readInput, channelIndex, actualIndex);
			invoke.setName("peekedItem");
			inst.replaceInstWithInsts(invoke, readIndices, readIndex, actualIndex, invoke);
		} else if (method.equals(pop2)) {
			Value channelIndex = inst.getArgument(1);
			LoadInst readIndices = new LoadInst(rwork.getLocalVariable("readIndex"));
			ArrayLoadInst readIndex = new ArrayLoadInst(readIndices, channelIndex);
			Argument readInput = rwork.getArgument("$readInput");
			CallInst invoke = new CallInst(invokeExact, module.types().getMethodType(Object.class, MethodHandle.class, int.class, int.class), readInput, channelIndex, readIndex);
			invoke.setName("poppedItem");
			BinaryInst incReadIndex = new BinaryInst(readIndex, BinaryInst.Operation.ADD, module.constants().getSmallestIntConstant(1));
			ArrayStoreInst storeReadIndex = new ArrayStoreInst(readIndices, channelIndex, incReadIndex);
			inst.replaceInstWithInsts(invoke, readIndices, readIndex, invoke, incReadIndex, storeReadIndex);
		} else if (method.equals(push2)) {
			Value channelIndex = inst.getArgument(1), item = inst.getArgument(2);
			LoadInst writeIndices = new LoadInst(rwork.getLocalVariable("writeIndex"));
			ArrayLoadInst writeIndex = new ArrayLoadInst(writeIndices, channelIndex);
			Argument writeOutput = rwork.getArgument("$writeOutput");
			CallInst invoke = new CallInst(invokeExact, module.types().getMethodType(void.class, MethodHandle.class, int.class, int.class, Object.class), writeOutput, channelIndex, writeIndex, item);
			BinaryInst incWriteIndex = new BinaryInst(writeIndex, BinaryInst.Operation.ADD, module.constants().getSmallestIntConstant(1));
			ArrayStoreInst storeWriteIndex = new ArrayStoreInst(writeIndices, channelIndex, incWriteIndex);
			inst.replaceInstWithInsts(invoke, writeIndices, writeIndex, invoke, incWriteIndex, storeWriteIndex);
		} else if (method.equals(inputs)) {
			LoadInst readIndex = new LoadInst(rwork.getLocalVariable("readIndex"));
			ArrayLengthInst length = new ArrayLengthInst(readIndex);
			inst.replaceInstWithInsts(length, readIndex, length);
		} else if (method.equals(outputs)) {
			LoadInst writeIndex = new LoadInst(rwork.getLocalVariable("writeIndex"));
			ArrayLengthInst length = new ArrayLengthInst(writeIndex);
			inst.replaceInstWithInsts(length, writeIndex, length);
		} else
			throw new AssertionError(inst);
	}

	private void remap(LoadInst inst) {
		assert inst.getLocation() instanceof Field;
		Argument replacement = inst.getParent().getParent().getArgument(inst.getLocation().getName());
		assert replacement != null;
		inst.replaceInstWithValue(replacement);
	}

	private void remap(StoreInst inst) {
		throw new UnsupportedOperationException("TODO: remap StoreInsts for stateful filters");
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
		MethodHandle handle = workMethod;
		//This relies on fields having a consistent iteration order.
		for (java.lang.reflect.Field f : fields)
			try {
				handle = MethodHandles.insertArguments(handle, 0, f.get(a.worker()));
			} catch (IllegalAccessException ex) {
				throw new AssertionError(ex);
			}
		return handle;
	}
}
