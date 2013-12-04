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
import edu.mit.streamjit.util.bytecode.insts.LoadInst;
import edu.mit.streamjit.util.bytecode.insts.ReturnInst;
import edu.mit.streamjit.util.bytecode.insts.StoreInst;
import java.lang.invoke.MethodHandle;
import java.util.EnumSet;
import java.util.List;

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
