package edu.mit.streamjit.util.bytecode;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Set;

/**
 * Represents an access kind.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/3/2013
 */
public enum Access {
	PUBLIC(Modifier.PUBLIC), PROTECTED(Modifier.PROTECTED),
	PACKAGE_PRIVATE(), PRIVATE(Modifier.PRIVATE);
	private static final ImmutableSet<Modifier> ACCESS_MODIFIERS =
			Sets.immutableEnumSet(Modifier.PUBLIC, Modifier.PROTECTED, Modifier.PRIVATE);
	private final ImmutableSet<Modifier> modifiers;
	private Access(Modifier... modifiers) {
		this.modifiers = Sets.immutableEnumSet(Arrays.asList(modifiers));
	}

	public ImmutableSet<Modifier> modifiers() {
		return modifiers;
	}

	public static Access fromModifiers(Set<Modifier> modifiers) {
		Set<Modifier> active = Sets.intersection(allAccessModifiers(), modifiers);
		for (Access a : values())
			if (active.equals(a.modifiers()))
				return a;
		throw new IllegalArgumentException("bad access modifiers: "+active);
	}

	public static ImmutableSet<Modifier> allAccessModifiers() {
		return ACCESS_MODIFIERS;
	}
}
