package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.primitives.Shorts;
import edu.mit.streamjit.util.IntrusiveList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * A Klass the the IR node representing a class, interface or primitive type;
 * basically, it's a symbolic representation of a Class object.
 * (The name "Class" was already taken.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/3/2013
 */
public class Klass implements Accessible, ParentedList.Parented<Module> {
	@IntrusiveList.Next
	private Klass next;
	@IntrusiveList.Previous
	private Klass previous;
	@ParentedList.Parent
	private Module parent;
	private final String name;
	private final Set<Modifier> modifiers;
	private final Klass superclass;
	private final List<Klass> interfaces;
	private final List<Field> fields;
	private final List<Method> methods;
	/**
	 * If this Klass represents a live Class, it's here.  Null otherwise.
	 */
	private final Class<?> backingClass;

	/**
	 * Creates a new mutable Klass instance.
	 * @param name
	 * @param superclass
	 * @param interfaces
	 * @param module
	 */
	public Klass(String name, Klass superclass, List<Klass> interfaces, Module module) {
		checkNotNull(name);
		checkNotNull(module);
		checkState(module.getKlassByName(name) == null, "klass named %s already in module");
		module.klasses().add(this); //sets parent
		this.name = name;
		this.modifiers = EnumSet.noneOf(Modifier.class);
		this.superclass = superclass;
		this.interfaces = interfaces == null ? new ArrayList<Klass>(2) : new ArrayList<>(interfaces);
		this.fields = new ParentedList<>(this, Field.class);
		this.methods = new ParentedList<>(this, Method.class);
		this.backingClass = null;
	}

	/**
	 * Creates a new immutable Klass instance representing the given class,
	 * recursively creating new Klass instances as required to resolve
	 * references from the given class.
	 * @param klass
	 * @param module
	 * @return
	 */
	public Klass(Class<?> klass, Module module) {
		checkNotNull(klass);
		checkNotNull(module);
		checkArgument(module.getKlassByName(klass.getName()) == null, "klass named %s already in module");
		//We're committed now.  Even through we aren't fully constructed,
		//register us with the module so any circular dependencies can find us.
		//Note that this means we can't use any of the klasses we recurse for
		//during our own construction!
		module.klasses().add(this); //sets parent
		this.name = klass.getName();
		this.modifiers = Sets.immutableEnumSet(Modifier.fromClassBits(Shorts.checkedCast(klass.getModifiers())));
		this.superclass = module.findOrConstruct(klass.getSuperclass());
		ImmutableList.Builder<Klass> interfacesB = ImmutableList.builder();
		for (Class<?> c : klass.getInterfaces())
			interfacesB.add(module.findOrConstruct(c));
		this.interfaces = interfacesB.build();
		ParentedList<Klass, Field> fieldList = new ParentedList<>(this, Field.class);
		for (java.lang.reflect.Field f : klass.getDeclaredFields())
			fieldList.add(new Field(f, this, module));
		this.fields = Collections.unmodifiableList(fieldList);
		//TODO: methods
		this.methods = Collections.unmodifiableList(new ParentedList<>(this, Method.class));
		this.backingClass = klass;
	}

	/**
	 * Returns true iff this Klass is mutable (not yet created as a live Class
	 * object).
	 * @return true iff this Klass is mutable
	 */
	public boolean isMutable() {
		return getBackingClass() == null;
	}

	/**
	 * If this Klass represents a class defined in a live Class object, returns
	 * that Class object.  Otherwise, returns null.
	 * @return the backing Class of this Klass, or null
	 */
	public Class<?> getBackingClass() {
		return backingClass;
	}

	public String getName() {
		return name;
	}
	public Set<Modifier> modifiers() {
		return modifiers;
	}
	public Klass getSuperclass() {
		return superclass;
	}
	public List<Klass> interfaces() {
		return interfaces;
	}
	public List<Field> fields() {
		return fields;
	}
	public List<Method> methods() {
		return methods;
	}

	public Field getFieldByName(String name) {
		for (Field f : fields())
			if (f.getName().equals(name))
				return f;
		return null;
	}

	@Override
	public Access getAccess() {
		return Access.fromModifiers(modifiers());
	}

	@Override
	public void setAccess(Access access) {
		modifiers().removeAll(Access.allAccessModifiers());
		modifiers().addAll(access.modifiers());
	}

	@Override
	public final Module getParent() {
		return parent;
	}

	@Override
	public String toString() {
		return getName();
	}

	public static void main(String[] args) {
		Module m = new Module();
		Klass klass = new Klass(ParentedList.class, m);
		System.out.println(klass);
	}
}
