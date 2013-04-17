package edu.mit.streamjit.impl.compiler;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import static com.google.common.base.Preconditions.*;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Shorts;
import edu.mit.streamjit.impl.compiler.types.MethodType;
import edu.mit.streamjit.impl.compiler.types.RegularType;
import edu.mit.streamjit.util.IntrusiveList;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A Klass the the IR node representing a class, interface or primitive type;
 * basically, it's a symbolic representation of a Class object.
 * (The name "Class" was already taken.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/3/2013
 */
public final class Klass implements Accessible, Parented<Module> {
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
		checkState(module.getKlass(name) == null, "klass named %s already in module");
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
		checkArgument(module.getKlass(klass.getName()) == null, "klass named %s already in module", klass.getName());

		this.backingClass = klass;
		this.name = klass.getName();
		this.modifiers = Sets.immutableEnumSet(Modifier.fromClassBits(Shorts.checkedCast(klass.getModifiers())));

		//We're committed now.  Even through we aren't fully constructed,
		//register us with the module so any circular dependencies can find us.
		//Note that this means we can't use any of the klasses we recurse for
		//during our own construction!
		module.klasses().add(this); //sets parent
		if (klass.getSuperclass() != null)
			this.superclass = module.getKlass(klass.getSuperclass());
		else if (klass.isInterface())
			//At the JVM level, interfaces have Object as superclass.
			this.superclass = module.getKlass(Object.class);
		else {
			assert klass.equals(Object.class) || klass.isPrimitive();
			this.superclass = null;
		}
		ImmutableList.Builder<Klass> interfacesB = ImmutableList.builder();
		for (Class<?> c : klass.getInterfaces())
			interfacesB.add(module.getKlass(c));
		this.interfaces = interfacesB.build();
		ParentedList<Klass, Field> fieldList = new ParentedList<>(this, Field.class);
		for (java.lang.reflect.Field f : klass.getDeclaredFields())
			fieldList.add(new Field(f, this, module));
		this.fields = Collections.unmodifiableList(fieldList);
		ParentedList<Klass, Method> methodList = new ParentedList<>(this, Method.class);
		for (java.lang.reflect.Constructor<?> c : klass.getDeclaredConstructors())
			methodList.add(new Method(c, this));
		for (java.lang.reflect.Method m : klass.getDeclaredMethods())
			methodList.add(new Method(m, this));
		this.methods = Collections.unmodifiableList(methodList);
	}

	/**
	 * Creates the array class with the given component type and (additional)
	 * dimensions.
	 * @param componentType
	 * @param dimensions
	 * @param module
	 */
	public Klass(Klass componentType, int dimensions, Module module) {
		checkNotNull(componentType);
		checkArgument(dimensions >= 1);
		checkNotNull(module);
		StringBuilder nameBuilder = new StringBuilder(Strings.repeat("[", dimensions));
		//Always a reference type; if not already an array, add L and ;.
		nameBuilder.append(componentType.isArray() ? componentType.getName() : "L" + componentType.getName() + ";");
		this.name = nameBuilder.toString();
		checkArgument(module.getKlass(name) == null, "array klass %s already in module", name);
		module.klasses().add(this); //sets parent
		//The access modifier for an array class is that of its element type.
		//Even if the element type is mutable, we only check the modifier once.
		this.modifiers = ImmutableSet.<Modifier>builder().addAll(componentType.getAccess().modifiers())
				.add(Modifier.ABSTRACT).add(Modifier.FINAL).build();
		this.superclass = module.getKlass(Object.class);
		this.interfaces = ImmutableList.of(module.getKlass(Cloneable.class), module.getKlass(Serializable.class));
		this.fields = ImmutableList.of();
		this.methods = ImmutableList.of();
		this.backingClass = null;
	}

	/**
	 * Returns true iff this Klass is mutable.  Klasses created from a Class
	 * object are immutable.  Klasses representing array classes, even arrays of
	 * mutable Klasses, are immutable.
	 * @return true iff this Klass is mutable
	 */
	public boolean isMutable() {
		return getBackingClass() == null && !isArray();
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

	/**
	 * Returns an iterable of all superclasses of this class, in ascending
	 * order; thus, the first class is the immediate superclass.
	 * @returnan an iterable of all superclasses of this class
	 */
	public Iterable<Klass> superclasses() {
		return new Iterable<Klass>() {
			@Override
			public Iterator<Klass> iterator() {
				return new AbstractSequentialIterator<Klass>(getSuperclass()) {
					@Override
					protected Klass computeNext(Klass previous) {
						return previous.getSuperclass();
					}
				};
			}
		};
	}

	public Field getField(String name) {
		for (Field f : fields())
			if (f.getName().equals(name))
				return f;
		return null;
	}

	public Method getMethod(String name, MethodType type) {
		for (Method m : methods())
			if (m.getName().equals(name) && m.getType().equals(type))
				return m;
		return null;
	}

	public Iterable<Method> getMethods(final String name) {
		return FluentIterable.from(methods()).filter(new Predicate<Method>() {
			@Override
			public boolean apply(Method input) {
				return input.getName().equals(name);
			}
		});
	}

	/**
	 * Looks up the method with the given name and type as though performing
	 * virtual dispatch.  That is, if this class does not contain the given
	 * method, it will be searched for in superclasses, with the required
	 * adjustment to the receiver type.
	 * @param name the name of the method to look up
	 * @param type the type of the method to look up, including the receiver as
	 * the first argument
	 * @return the method that would be called by virtual dispatch, or null if
	 * no method matches
	 */
	public Method getMethodByVirtual(String name, MethodType type) {
		/* TODO: this doesn't correctly enforce the visibility rules for
		 * overrides -- see JVMS 5.4.5. */
		//We require an exact match on the non-receiver args.
		MethodType descriptorType = type.dropFirstArgument();
		RegularType receiver = type.getParameterTypes().get(0);
		for (Klass k = this; k != null; k = k.getSuperclass()) {
			for (Method m : k.getMethods(name)) {
				if (m == null || m.modifiers().contains(Modifier.STATIC))
					continue;
				MethodType desc = m.getType().dropFirstArgument();
				RegularType rec = m.getType().getParameterTypes().get(0);
				if (desc.equals(descriptorType) && receiver.isSubtypeOf(rec))
					return m;
			}
		}
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

	public final boolean isArray() {
		return getName().startsWith("[");
	}

	/**
	 * If this Klass represents an array class, returns the number of array
	 * dimensions; otherwise, returns 0.
	 * @return the number of dimensions, or 0
	 */
	public final int getDimensions() {
		return getName().lastIndexOf('[')+1;
	}

	/**
	 * Returns the component Klass of this array class; that is, the Klass with
	 * one fewer dimension (possibly with zero dimensions).  Thus [[I becomes
	 * [I.
	 * @return the component Klass of this array class
	 * @throws IllegalStateException if this Klass is not an array class
	 */
	public final Klass getComponentKlass() {
		checkState(isArray(), "not array class: %s", getName());
		int nd = getDimensions()-1;
		if (nd == 0)
			return getElementKlass();
		else
			return getParent().getArrayKlass(getElementKlass(), getDimensions()-1);
	}

	/**
	 * Returns the element Klass of this array class; that is, the Klass with
	 * all dimensions stripped off.  Thus [[I becomes I.
	 * @return the element Klass of this array class
	 * @throws IllegalStateException if this Klass is not an array class
	 */
	public final Klass getElementKlass() {
		checkState(isArray(), "not array class: %s", getName());
		if (getBackingClass() != null) {
			Class<?> b = getBackingClass();
			while (b.getComponentType() != null)
				b = b.getComponentType();
			return getParent().getKlass(b);
		}
		//Our element type is mutable, so we must have already created it.
		String elementName = getName().replaceAll("\\[*L(.*);", "$1");
		Klass elementKlass = getParent().getKlass(elementName);
		assert elementKlass != null;
		return elementKlass;
	}

	@Override
	public final Module getParent() {
		return parent;
	}

	@Override
	public String toString() {
		return getName();
	}

	public void dump(PrintWriter writer) {
		writer.write(Joiner.on(' ').join(modifiers()));
		writer.write(" ");
		writer.write(getName());
		writer.println();
		if (getSuperclass() != null) {
			writer.write('\t');
			writer.write("extends ");
			writer.write(Joiner.on(", ").join(superclasses()));
			writer.println();
		}
		if (!interfaces().isEmpty()) {
			writer.write('\t');
			writer.write("implements ");
			writer.write(Joiner.on(", ").join(FluentIterable.from(interfaces()).transform(new Function<Klass, String>() {
				@Override
				public String apply(Klass input) {
					return input.getName();
				}
			})));
			writer.println();
		}
		writer.println();

		for (Field f : fields)
			writer.println(f.toString());
		writer.println();

		for (Method m : methods)
			m.dump(writer);

		writer.flush();
	}

	public static void main(String[] args) {
		Module m = new Module();
		System.out.println(new Klass(ParentedList.class, m));
		m.getKlass(ParentedList.class).dump(new PrintWriter(System.out, true));
		System.out.println(new Klass(ImmutableList.class, m));
		System.out.println(new Klass(javax.swing.JCheckBoxMenuItem.class, m));
		System.out.println(new Klass(java.nio.file.Files.class, m));
		System.out.println(new Klass(javax.net.ssl.SSLSocket.class, m));
		System.out.println(m.klasses().size()+" classes reflectively parsed");
		System.out.println(m.constants().getNullConstant());
		System.out.println(m.constants().getSmallestIntConstant(-255));
	}
}
