package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Shorts;
import java.util.EnumSet;
import java.util.Set;
import org.objectweb.asm.Opcodes;

/**
 * Represents a JVM-level modifier.  See JVMS 4.1 (classes), 4.5 (fields), and
 * 4.6 (methods).
 *
 * TODO: beyond just class/field/method, model the extra relationships for
 * special methods (class/instance initialization, interface methods), interface
 * fields, etc., including both overall restrictions, conditional restrictions/
 * requirements (e.g., all interface fields are public static final), and
 * mutually exclusive modifiers (no "volatile final").
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/1/2013
 */
public enum Modifier {
	PUBLIC(Opcodes.ACC_PUBLIC),
	PRIVATE(Opcodes.ACC_PRIVATE),
	PROTECTED(Opcodes.ACC_PROTECTED),
	STATIC(Opcodes.ACC_STATIC),
	FINAL(Opcodes.ACC_FINAL),
	SUPER(Opcodes.ACC_SUPER),
	SYNCHRONIZED(Opcodes.ACC_SYNCHRONIZED),
	VOLATILE(Opcodes.ACC_VOLATILE),
	BRIDGE(Opcodes.ACC_BRIDGE),
	VARARGS(Opcodes.ACC_VARARGS),
	TRANSIENT(Opcodes.ACC_TRANSIENT),
	NATIVE(Opcodes.ACC_NATIVE),
	INTERFACE(Opcodes.ACC_INTERFACE),
	ABSTRACT(Opcodes.ACC_ABSTRACT),
	STRICT(Opcodes.ACC_STRICT),
	SYNTHETIC(Opcodes.ACC_SYNTHETIC),
	ANNOTATION(Opcodes.ACC_ANNOTATION),
	ENUM(Opcodes.ACC_ENUM);
	private static final ImmutableSet<Modifier> CLASS_MODIFIERS = Sets.immutableEnumSet(
			PUBLIC, FINAL, SUPER, INTERFACE, ABSTRACT, SYNTHETIC, ANNOTATION, ENUM
	);
	private static final ImmutableSet<Modifier> FIELD_MODIFIERS = Sets.immutableEnumSet(
			PUBLIC, PRIVATE, PROTECTED, STATIC, FINAL, VOLATILE, TRANSIENT,
			SYNTHETIC, ENUM
	);
	private static final ImmutableSet<Modifier> METHOD_MODIFIERS = Sets.immutableEnumSet(
			PUBLIC, PRIVATE, PROTECTED, STATIC, FINAL, SYNCHRONIZED, BRIDGE,
			VARARGS, NATIVE, ABSTRACT, STRICT, SYNTHETIC
	);
	private final short bit;
	private Modifier(int bit) {
		this.bit = Shorts.checkedCast(bit);
	}

	/**
	 * Returns this modifier's flag bit.
	 * @return this modifier's flag bit
	 */
	public short bit() {
		return bit;
	}

	/**
	 * Returns true iff this modifier can appear on a class.
	 * @return true iff this modifier can appear on a class
	 */
	public boolean canModifyClass() {
		return CLASS_MODIFIERS.contains(this);
	}

	/**
	 * Returns true iff this modifier can appear on a field.
	 * @return true iff this modifier can appear on a field
	 */
	public boolean canModifyField() {
		return FIELD_MODIFIERS.contains(this);
	}

	/**
	 * Returns true iff this modifier can appear on a method.
	 * @return true iff this modifier can appear on a method
	 */
	public boolean canModifyMethod() {
		return METHOD_MODIFIERS.contains(this);
	}

	public static EnumSet<Modifier> fromClassBits(short bits) {
		return fromBits(bits, CLASS_MODIFIERS);
	}

	public static EnumSet<Modifier> fromFieldBits(short bits) {
		return fromBits(bits, FIELD_MODIFIERS);
	}

	public static EnumSet<Modifier> fromMethodBits(short bits) {
		return fromBits(bits, METHOD_MODIFIERS);
	}

	private static EnumSet<Modifier> fromBits(short bits, Set<Modifier> legalModifiers) {
		EnumSet<Modifier> modifiers = EnumSet.noneOf(Modifier.class);
		for (Modifier m : legalModifiers) {
			if ((bits & m.bit()) != 0) {
				modifiers.add(m);
				//Mask out the bit.
				bits &= ~m.bit();
			}
		}
		//Did we get all the modifiers?
		checkArgument(bits == 0, "illegal bits: %s", bits);
		return modifiers;
	}

	public static short toBits(Set<Modifier> modifiers) {
		short s = 0;
		for (Modifier m : modifiers)
			s |= m.bit();
		return s;
	}

	/**
	 * Returns true iff the given set of modifiers represents a package-private
	 * ("default access") element.
	 * @param modifiers an element's set of modifiers
	 * @return true iff the element is package-private
	 */
	public static boolean isPackagePrivate(Set<Modifier> modifiers) {
		return !modifiers.contains(PUBLIC) && !modifiers.contains(PRIVATE) && !modifiers.contains(PROTECTED);
	}
}
