package edu.mit.streamjit.util;

import static com.google.common.base.Preconditions.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.List;

/**
 * Contains various combinators and other MethodHandle utilities.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/3/2013
 */
public final class Combinators {
	private Combinators() {}

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

	private static final MethodHandle METHODHANDLE_ARRAY_GETTER = MethodHandles.arrayElementGetter(MethodHandle[].class);
	/**
	 * Returns a MethodHandle with a leading int argument that selects one of
	 * the MethodHandles in the given array, which is invoked with the
	 * remaining arguments.
	 * @param cases the cases to select from
	 * @return a MethodHandle approximating the switch statement
	 */
	public static MethodHandle tableswitch(MethodHandle[] cases) {
		checkArgument(cases.length >= 1);
		MethodType type = cases[0].type();
		for (MethodHandle mh : cases)
			checkArgument(mh.type().equals(type), "Type mismatch in "+Arrays.toString(cases));
		MethodHandle selector = METHODHANDLE_ARRAY_GETTER.bindTo(cases);
		//Replace the index with the handle to invoke, passing it to an invoker.
		return MethodHandles.filterArguments(MethodHandles.exactInvoker(type), 0, selector);
	}

	private static final MethodHandle SEMICOLON;
	static {
		try {
			SEMICOLON = LOOKUP.findStatic(Combinators.class, "SEMICOLON",
					MethodType.methodType(void.class, MethodHandle[].class));
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError("Can't happen!", ex);
		}
	}
	private static void _semicolon(MethodHandle... handles) throws Throwable {
		for (MethodHandle m : handles)
			m.invoke();
	}

	/**
	 * Returns a MethodHandle that calls the given no-arg MethodHandles in
	 * sequence, ignoring any return values.  If no handles are given, the
	 * returned handle does nothing
	 * @param handles the handles to invoke
	 * @return a MethodHandle approximating semicolons
	 */
	public static MethodHandle semicolon(MethodHandle... handles) {
		for (int i = 0; i < handles.length; ++i)
			handles[i] = handles[i].asType(MethodType.methodType(void.class));
		return SEMICOLON.bindTo(handles);
	}

	/**
	 * Returns a MethodHandle that calls the given no-arg MethodHandles in
	 * sequence, ignoring any return values.  If no handles are given, the
	 * returned handle does nothing
	 * @param handles the handles to invoke
	 * @return a MethodHandle approximating semicolons
	 */
	public static MethodHandle semicolon(List<MethodHandle> handles) {
		return semicolon(handles.toArray(new MethodHandle[0]));
	}

	private static final MethodHandle VOID_VOID_NOP = MethodHandles.identity(Object.class)
			.bindTo(null).asType(MethodType.methodType(void.class));
	/**
	 * Returns a MethodHandle that takes the given argument types, returns void,
	 * and does nothing when called.
	 * @param arguments the argument types
	 * @return a no-op method handle
	 */
	public static MethodHandle nop(Class<?>... arguments) {
		return MethodHandles.dropArguments(VOID_VOID_NOP, 0, arguments);
	}
}
