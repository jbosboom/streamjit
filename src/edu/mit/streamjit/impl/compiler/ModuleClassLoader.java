package edu.mit.streamjit.impl.compiler;

/**
 * A ModuleClassLoader loads classes from a Module.
 *
 * TODO: This loader keeps the Module alive as long as any classes it loads are
 * alive.  We may wish to load classes eagerly, then discard the Module.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/23/2013
 */
public final class ModuleClassLoader extends ClassLoader {
	private final Module module;
	/**
	 * Creates a new ModuleClassLoader that will load classes from the given
	 * module after delegating to the current thread's context class loader.
	 * @param module the module to load classes from
	 */
	public ModuleClassLoader(Module module) {
		this(module, Thread.currentThread().getContextClassLoader());
	}

	/**
	 * Creates a new ModuleClassLoader that will load classes from the given
	 * module after delegating to the given class loader.
	 * @param module the module to load classes from
	 * @param parent the parent class loader
	 */
	public ModuleClassLoader(Module module, ClassLoader parent) {
		super(parent);
		this.module = module;
	}

	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		Klass klass = module.getKlass(name);
		if (klass == null)
			throw new ClassNotFoundException(name);
		byte[] bytes = KlassUnresolver.unresolve(klass);
		return defineClass(name, bytes, 0, bytes.length);
	}

	public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		Module m = new Module();
		Klass k = new Klass("foo.MyClass", m.getKlass(Object.class), null, m);
		System.out.println(m.klasses());
		ModuleClassLoader cl = new ModuleClassLoader(m);
		Class<?> l = cl.loadClass(k.getName());
		System.out.println(l);
		System.out.println(l.newInstance());
	}
}
