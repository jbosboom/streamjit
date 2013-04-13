package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Strings;
import edu.mit.streamjit.impl.compiler.types.TypeFactory;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Module is the top-level IR node for a single compilation, analogous to a
 * translation unit in other compilers.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public final class Module {
	private final TypeFactory typeFactory = new TypeFactory(this);
	private final ConstantFactory constantFactory = new ConstantFactory(this);
	private KlassList klasses = new KlassList(this);
	private Map<String, Klass> klassesMap = new HashMap<>();
	public Module() {
	}

	public List<Klass> klasses() {
		return klasses;
	}

	public TypeFactory types() {
		return typeFactory;
	}

	public ConstantFactory constants() {
		return constantFactory;
	}

	/**
	 * Gets the Klass with the given name, or null if this Module doesn't
	 * contain a Klass with the given name.
	 * @param name the name of the Klass to get
	 * @return the Klass with the given name, or null
	 */
	public Klass getKlass(String name) {
		checkNotNull(name);
		return klassesMap.get(name);
	}

	/**
	 * Gets the Klass representing the given Class object, creating and adding
	 * it to this module if necessary.
	 * @param klass the class to get a Klass for
	 * @return a Klass representing the given Class
	 */
	public Klass getKlass(Class<?> klass) {
		Klass klassByName = getKlass(klass.getName());
		if (klassByName != null)
			return klassByName;
		return new Klass(klass, this);
	}

	public Klass getArrayKlass(Klass componentType, int dimensions) {
		checkNotNull(componentType);
		checkArgument(dimensions >= 1);
		if (componentType.getBackingClass() != null)
			return getKlass(Array.newInstance(componentType.getBackingClass(), new int[dimensions]).getClass());
		StringBuilder nameBuilder = new StringBuilder(Strings.repeat("[", dimensions));
		//Always a reference type; if not already an array, add L and ;.
		nameBuilder.append(componentType.isArray() ? componentType.getName() : "L" + componentType.getName() + ";");
		String name = nameBuilder.toString();
		Klass alreadyExists = getKlass(name);
		if (alreadyExists != null)
			return alreadyExists;
		return new Klass(componentType, dimensions, this);
	}

	/**
	 * Ensures we don't end up with two classes with the same name in the list.
	 */
	private class KlassList extends ParentedList<Module, Klass> {
		private KlassList(Module parent) {
			super(parent, Klass.class);
		}
		@Override
		protected void elementAdding(Klass t) {
			checkArgument(!klassesMap.containsKey(t.getName()), "adding duplicate %s", t.getName());
			super.elementAdding(t);
		}
		@Override
		protected void elementAdded(Klass t) {
			super.elementAdded(t);
			klassesMap.put(t.getName(), t);
		}
		@Override
		protected void elementRemoving(Klass t) {
			//Removing an immutable Klass makes no sense as any replacement
			//would have no effect (because the module-based ClassLoader prefers
			//classes loaded by the parent) and could break other mutable
			//Klasses if they depended on the changed definitions.
			checkArgument(t.isMutable(), "removing immutable Klass %s", t.getName());
			super.elementRemoving(t);
		}

		@Override
		protected void elementRemoved(Klass t) {
			super.elementRemoved(t);
			Klass removed = klassesMap.remove(t.getName());
			assert t.equals(removed) : t +", "+removed;
		}
	}
}
