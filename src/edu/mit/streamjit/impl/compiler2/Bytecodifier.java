package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.util.bytecode.BasicBlock;
import edu.mit.streamjit.util.bytecode.Field;
import edu.mit.streamjit.util.bytecode.Klass;
import edu.mit.streamjit.util.bytecode.Method;
import edu.mit.streamjit.util.bytecode.Modifier;
import edu.mit.streamjit.util.bytecode.Module;
import edu.mit.streamjit.util.bytecode.ModuleClassLoader;
import edu.mit.streamjit.util.bytecode.insts.CallInst;
import edu.mit.streamjit.util.bytecode.insts.CastInst;
import edu.mit.streamjit.util.bytecode.insts.LoadInst;
import edu.mit.streamjit.util.bytecode.insts.ReturnInst;
import edu.mit.streamjit.util.bytecode.insts.StoreInst;
import edu.mit.streamjit.util.bytecode.types.VoidType;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bytecodifies MethodHandles.  That is, stores them in a static final field,
 * spins a method that calls through that field, and returns a MethodHandle
 * to that new method.  The effect is to provide a specialization point for the
 * JVM to inline into.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 12/3/2013
 */
public final class Bytecodifier {
	private Bytecodifier() {}

	/**
	 * A "bounce point" for setting up static final fields in classes we spin.
	 * We generate a new integer and store our state in the map, while the spun
	 * class removes it in a static initializer.  This avoids spinning an extra
	 * "field helper" class.
	 *
	 * The keys are all stringized integers; we use String to avoid having to
	 * generate boxing code for an integer constant.
	 */
	public static final ConcurrentHashMap<String, MethodHandle> BOUNCER = new ConcurrentHashMap<>();
	private static final AtomicInteger BOUNCER_KEY_FACTORY = new AtomicInteger();
	private static final MethodHandles.Lookup PUBLIC_LOOKUP = MethodHandles.publicLookup();

	/**
	 * Returns a MethodHandle that redirects through a newly-spun static method
	 * before calling the given handle.  The returned handle is behaviorally
	 * identical to the given handle.
	 * @param handle the handle to bytecodify
	 * @param className the name of the class to be spun; must be unique within
	 * the module
	 * @param module the module to spin the class in
	 * @param loader the class loader to load the class with
	 * @return a behaviorally identical MethodHandle
	 */
	public static MethodHandle bytecodify(MethodHandle handle, String className, Module module, ModuleClassLoader loader) {
		Klass klass = new Klass(className, module.getKlass(Object.class), ImmutableList.<Klass>of(), module);
		klass.modifiers().addAll(EnumSet.of(Modifier.PUBLIC, Modifier.FINAL));
		Field field = new Field(module.types().getRegularType(MethodHandle.class), "handle", EnumSet.of(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL), klass);

		String key = String.valueOf(BOUNCER_KEY_FACTORY.getAndIncrement());
		MethodHandle previousHandle = BOUNCER.putIfAbsent(key, handle);
		assert previousHandle == null : "reused "+key;
		Method clinit = new Method("<clinit>",
				module.types().getMethodType(void.class),
				EnumSet.of(Modifier.STATIC),
				klass);
		BasicBlock clinitBlock = new BasicBlock(module);
		clinit.basicBlocks().add(clinitBlock);
		LoadInst loadBouncer = new LoadInst(module.getKlass(Bytecodifier.class).getField("BOUNCER"));
		Method chmGet = Iterables.getOnlyElement(module.getKlass(ConcurrentHashMap.class).getMethods("get"));
		CallInst get = new CallInst(chmGet, loadBouncer, module.constants().getConstant(key));
		CastInst unerase = new CastInst(field.getType().getFieldType(), get);
		StoreInst storeField = new StoreInst(field, unerase);
		clinitBlock.instructions().addAll(ImmutableList.of(loadBouncer, get, unerase, storeField,
				new ReturnInst(module.types().getVoidType())));

		Method invoke = new Method("$invoke",
				module.types().getMethodType(handle.type()),
				EnumSet.of(Modifier.PUBLIC, Modifier.STATIC),
				klass);
		BasicBlock invokeBlock = new BasicBlock(module);
		invoke.basicBlocks().add(invokeBlock);
		LoadInst loadHandle = new LoadInst(field);
		Method invokeExact = Iterables.getOnlyElement(module.getKlass(MethodHandle.class).getMethods("invokeExact"));
		CallInst call = new CallInst(invokeExact, invoke.getType().prependArgument(module.types().getRegularType(MethodHandle.class)), loadHandle);
		for (int i = 0; i < invoke.arguments().size(); ++i)
			call.setArgument(i+1, invoke.arguments().get(i));
		invokeBlock.instructions().addAll(ImmutableList.of(loadHandle, call,
				call.getType() instanceof VoidType ?
						new ReturnInst(module.types().getVoidType()) :
						new ReturnInst(invoke.getType().getReturnType(), call)));

		try {
			Class<?> liveClass = loader.loadClass(className);
			//We should have removed this during static initialization.
			assert !BOUNCER.containsKey(key) : key+" not removed from bouncer";
			return PUBLIC_LOOKUP.findStatic(liveClass, "$invoke", handle.type());
		} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError(ex);
		}
	}

	public static ImmutableList<Runnable> runnableProxies(List<MethodHandle> handles) {
		ImmutableList.Builder<String> names = ImmutableList.builder();
		for (int i = 0; i < handles.size(); ++i)
			names.add("Proxy"+i);
		return runnableProxies(handles, names.build());
	}

	public static ImmutableList<Runnable> runnableProxies(List<MethodHandle> handles, List<String> proxyNames) {
		try {
			Module m = new Module();
			ModuleClassLoader mcl = new ModuleClassLoader(m);
			Klass fieldHelperKlass = new Klass("ProxyFieldHelper", m.getKlass(Object.class), ImmutableList.<Klass>of(), m);
			fieldHelperKlass.modifiers().addAll(EnumSet.of(Modifier.FINAL)); //TODO: public?
			for (int i = 0; i < handles.size(); ++i)
				new Field(m.types().getRegularType(MethodHandle.class), "handle"+i, EnumSet.of(Modifier.PUBLIC, Modifier.STATIC), fieldHelperKlass);
			Class<?> fieldHelper = mcl.loadClass(fieldHelperKlass.getName());
			for (int i = 0; i < handles.size(); ++i) {
				java.lang.reflect.Field field = fieldHelper.getField("handle"+i);
				field.setAccessible(true);
				field.set(null, handles.get(i));
			}

			ImmutableList.Builder<Runnable> builder = ImmutableList.builder();
			for (int i = 0; i < handles.size(); ++i) {
				Klass proxyKlass = new Klass(proxyNames.get(i), m.getKlass(Object.class), ImmutableList.of(m.getKlass(Runnable.class)), m);
				proxyKlass.modifiers().addAll(EnumSet.of(Modifier.PUBLIC, Modifier.FINAL));
				Field handle = new Field(m.types().getRegularType(MethodHandle.class), "handle"+i, EnumSet.of(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL), proxyKlass);

				Method clinit = new Method("<clinit>",
						m.types().getMethodType(void.class),
						EnumSet.of(Modifier.STATIC),
						proxyKlass);
				BasicBlock clinitBlock = new BasicBlock(m);
				clinit.basicBlocks().add(clinitBlock);
				LoadInst loadHelperField = new LoadInst(fieldHelperKlass.getField("handle"+i));
				StoreInst storeProxyField = new StoreInst(handle, loadHelperField);
				clinitBlock.instructions().addAll(ImmutableList.of(loadHelperField, storeProxyField,
						new ReturnInst(m.types().getVoidType())));

				Method init = new Method("<init>",
						m.types().getMethodType(m.types().getType(proxyKlass)),
						EnumSet.of(Modifier.PUBLIC),
						proxyKlass);
				BasicBlock initBlock = new BasicBlock(m);
				init.basicBlocks().add(initBlock);
				Method objCtor = m.getKlass(Object.class).getMethods("<init>").iterator().next();
				initBlock.instructions().add(new CallInst(objCtor));
				initBlock.instructions().add(new ReturnInst(m.types().getVoidType()));

				Method run = new Method("run",
						m.types().getMethodType(void.class).appendArgument(m.types().getRegularType(proxyKlass)),
						EnumSet.of(Modifier.PUBLIC, Modifier.FINAL),
						proxyKlass);
				BasicBlock runBlock = new BasicBlock(m);
				run.basicBlocks().add(runBlock);
				LoadInst li = new LoadInst(handle);
				runBlock.instructions().add(li);
				runBlock.instructions().add(new CallInst(
						Iterables.getOnlyElement(m.getKlass("java.lang.invoke.MethodHandle").getMethods("invokeExact")),
						m.types().getMethodType(void.class, MethodHandle.class),
						li));
				runBlock.instructions().add(new ReturnInst(m.types().getVoidType()));

				Class<?> proxy = mcl.loadClass(proxyKlass.getName());
				builder.add((Runnable)proxy.newInstance());
			}
			return builder.build();
		} catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException | InstantiationException ex) {
			throw new AssertionError(ex);
		}
	}
}
