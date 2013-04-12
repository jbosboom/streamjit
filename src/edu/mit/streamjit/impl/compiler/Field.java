package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.impl.compiler.types.RegularType;
import static com.google.common.base.Preconditions.*;
import com.google.common.collect.Sets;
import com.google.common.primitives.Shorts;
import edu.mit.streamjit.util.IntrusiveList;
import java.util.EnumSet;
import java.util.Set;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/3/2013
 */
public final class Field implements Accessible, Parented<Klass> {
	@IntrusiveList.Previous
	private Field previous;
	@IntrusiveList.Next
	private Field next;
	@ParentedList.Parent
	private Klass parent;
	private final String name;
	private final RegularType type;
	private final Set<Modifier> modifiers;
	public Field(java.lang.reflect.Field f, Klass parent, Module module) {
		//parent is set by our parent adding us to its list prior to making it
		//unmodifiable.  (We can't add ourselves and have the list wrapped
		//unmodifiable later because it's stored in a final field.)
		this.name = f.getName();
		this.type = module.types().getRegularType(module.getKlass(f.getType()));
		this.modifiers = Sets.immutableEnumSet(Modifier.fromFieldBits(Shorts.checkedCast(f.getModifiers())));
	}
	public Field(RegularType type, String name, Set<Modifier> modifiers, Klass parent) {
		checkNotNull(type);
		checkNotNull(name);
		checkNotNull(modifiers);
		this.name = name;
		this.type = type;
		this.modifiers = EnumSet.noneOf(Modifier.class);
		modifiers().addAll(modifiers);
		if (parent != null)
			parent.fields().add(this);
	}

	public boolean isMutable() {
		return (parent == null) || parent.isMutable();
	}

	public java.lang.reflect.Field getBackingField() {
		//We don't call this very often (if at all), so look it up every time
		//rather than burn a field on all Fields.
		Class<?> klass = getParent().getBackingClass();
		try {
			return klass != null ? klass.getDeclaredField(getName()) : null;
		} catch (NoSuchFieldException ex) {
			throw new AssertionError(String.format("Can't happen! Class %s doesn't have a %s field?", klass, getName()), ex);
		}
	}

	public String getName() {
		return name;
	}
	public RegularType getType() {
		return type;
	}
	public Set<Modifier> modifiers() {
		return modifiers;
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
	public Klass getParent() {
		return parent;
	}
}
