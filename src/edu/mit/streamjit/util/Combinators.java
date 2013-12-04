package edu.mit.streamjit.util;

import static com.google.common.base.Preconditions.*;
import static edu.mit.streamjit.util.LookupUtils.findStatic;
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
	private static final MethodHandle ADD = findStatic(LOOKUP, Combinators.class, "_add", INT_INT_TO_INT);
	private static final MethodHandle SUB = findStatic(LOOKUP, Combinators.class, "_sub", INT_INT_TO_INT);
	private static final MethodHandle MUL = findStatic(LOOKUP, Combinators.class, "_mul", INT_INT_TO_INT);
	private static final MethodHandle DIV = findStatic(LOOKUP, Combinators.class, "_div", INT_INT_TO_INT);
	private static final MethodHandle REM = findStatic(LOOKUP, Combinators.class, "_rem", INT_INT_TO_INT);
	private static final MethodHandle MOD = findStatic(LOOKUP, Combinators.class, "_mod", INT_INT_TO_INT);
	private static int _add(int x, int y) {
		return x + y;
	}
	private static int _sub(int x, int y) {
		return x - y;
	}
	private static int _mul(int x, int y) {
		return x * y;
	}
	private static int _div(int x, int y) {
		return x / y;
	}
	private static int _rem(int x, int y) {
		return x % y;
	}
	private static int _mod(int x, int y) {
		int r = x % y;
		return r >= 0 ? r : r + y;
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
	public static MethodHandle rem(MethodHandle x, int y) {
		return _filterIntIntToInt(x, REM, y);
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

	/**
	 * Returns a MethodHandle that calls the given method handles in sequence,
	 * ignoring their return values.  The given handles must all take the same
	 * parameters.  They may have any return type, but any returned values will
	 * be ignored.  If no handles are given, the returned handle does nothing.
	 * @param handles the handles to invoke
	 * @return a MethodHandle approximating semicolons
	 */
	public static MethodHandle semicolon(MethodHandle... handles) {
		if (handles.length == 0)
			return nop();
		MethodType type = handles[0].type().changeReturnType(void.class);
		if (handles.length == 1)
			return handles[0].asType(type);
		MethodHandle chain = nop(type.parameterArray());
		for (int i = handles.length-1; i >= 0; --i) {
			checkArgument(handles[i].type().parameterList().equals(type.parameterList()), "Type mismatch in "+Arrays.toString(handles));
			chain = MethodHandles.foldArguments(chain, handles[i].asType(type));
		}
		return chain;
	}

	/**
	 * Returns a MethodHandle that calls the given method handles in sequence,
	 * ignoring their return values.  The given handles must all take the same
	 * parameters.  They may have any return type, but any returned values will
	 * be ignored.  If no handles are given, the returned handle does nothing.
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
