package edu.mit.streamjit.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;

/**
 * Contains various combinators and other MethodHandle utilities.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/3/2013
 */
public final class Combinators {
	private Combinators() {}

	public static MethodHandle add(MethodHandle x, int y) {
		return _filterIntIntToInt(x, ADD, y);
	}
	public static MethodHandle sub(MethodHandle x, int y) {
		return _filterIntIntToInt(x, SUB, y);
	}
	public static MethodHandle mul(MethodHandle x, int y) {
		return _filterIntIntToInt(x, MUL, y);
	}
	public static MethodHandle div(MethodHandle x, int y) {
		return _filterIntIntToInt(x, DIV, y);
	}
	public static MethodHandle mod(MethodHandle x, int y) {
		return _filterIntIntToInt(x, MOD, y);
	}

	private static final Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodType INT_INT_TO_INT = MethodType.methodType(int.class, int.class, int.class);
	private static final MethodHandle ADD, SUB, MUL, DIV, MOD;
	static {
		try {
			ADD = LOOKUP.findStatic(Combinators.class, "_add", INT_INT_TO_INT);
			SUB = LOOKUP.findStatic(Combinators.class, "_sub", INT_INT_TO_INT);
			MUL = LOOKUP.findStatic(Combinators.class, "_mul", INT_INT_TO_INT);
			DIV = LOOKUP.findStatic(Combinators.class, "_div", INT_INT_TO_INT);
			MOD = LOOKUP.findStatic(Combinators.class, "_mod", INT_INT_TO_INT);
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError("Can't happen!", ex);
		}
	}
	private static int _add(int x, int y) {
		return x + y;
	}
	private static int _sub(int x, int y) {
		return x - y;
	}
	private static int _mul(int x, int y) {
		return x % y;
	}
	private static int _div(int x, int y) {
		return x % y;
	}
	private static int _mod(int x, int y) {
		return x % y;
	}
	private static MethodHandle _filterIntIntToInt(MethodHandle target, MethodHandle filter, int filterSecondArg) {
		return MethodHandles.filterReturnValue(target, MethodHandles.insertArguments(filter, 1, filterSecondArg));
	}
}
