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
		Method chmRemove = module.getKlass(ConcurrentHashMap.class).getMethod("remove", module.types().getMethodType(Object.class, ConcurrentHashMap.class, Object.class));
		CallInst remove = new CallInst(chmRemove, loadBouncer, module.constants().getConstant(key));
		CastInst unerase = new CastInst(field.getType().getFieldType(), remove);
		StoreInst storeField = new StoreInst(field, unerase);
		clinitBlock.instructions().addAll(ImmutableList.of(loadBouncer, remove, unerase, storeField,
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
			return PUBLIC_LOOKUP.findStatic(liveClass, "$invoke", handle.type());
		} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError(ex);
		}
	}

	public static final class Function {
		private final Module module;
		private final ModuleClassLoader loader;
		private final String packagePrefix;
		public Function(Module module, ModuleClassLoader loader, String packagePrefix) {
			this.module = module;
			this.loader = loader;
			this.packagePrefix = packagePrefix.endsWith(".") ? packagePrefix : packagePrefix + ".";
		}
		public MethodHandle bytecodify(MethodHandle handle, String className) {
			return Bytecodifier.bytecodify(handle, packagePrefix + className, module, loader);
		}
	}
}
