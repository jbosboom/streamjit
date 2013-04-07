package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import java.util.List;

/**
 * Module is the top-level IR node for a single compilation, analogous to a
 * translation unit in other compilers.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/6/2013
 */
public final class Module {
	private KlassList klasses = new KlassList(this);
	public Module() {
	}

	public List<Klass> klasses() {
		return klasses;
	}

	public Klass getKlassByName(String name) {
		for (Klass klass : klasses())
			if (klass.getName().equals(name))
				return klass;
		return null;
	}

	/**
	 * Returns the Klass for the given Class if we've already created it (or are
	 * in the process of creating it), or creates and returns it otherwise.
	 *
	 * For internal use only during creation of Klasses from Classes.
	 * @param klass a Class
	 * @return a Klass
	 */
	final Klass findOrConstruct(Class<?> klass) {
		if (klass == null)
			return null;
		Klass klassByName = getKlassByName(klass.getName());
		if (klassByName != null)
			return klassByName;
		return new Klass(klass, this);
	}

	/**
	 * Ensures we don't end up with two classes with the same name in the list.
	 */
	private static class KlassList extends ParentedList<Module, Klass> {
		private KlassList(Module parent) {
			super(parent, Klass.class);
		}
		@Override
		protected void elementAdding(Klass t) {
			for (Klass klass : this)
				checkArgument(!klass.getName().equals(t.getName()), "adding duplicate %s", t.getName());
			super.elementAdding(t);
		}
	}
}
