package edu.mit.streamjit.util.bytecode;

import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.util.bytecode.types.Type;
import edu.mit.streamjit.util.bytecode.types.WrapperType;
import edu.mit.streamjit.util.ReflectionUtils;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A ConstantFactory makes Constants on behalf of a Module.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/12/2013
 */
public class ConstantFactory implements Iterable<Constant<?>> {
	private final Module parent;
	private final Map<Object, Constant<?>> constantsMap = new HashMap<>();

	ConstantFactory(Module parent) {
		assert ReflectionUtils.calledDirectlyFrom(Module.class);
		this.parent = checkNotNull(parent);
	}

	public Constant<Boolean> getConstant(boolean c) {
		return getConstant(c, Boolean.class);
	}
	public Constant<Byte> getConstant(byte c) {
		return getConstant(c, Byte.class);
	}
	public Constant<Character> getConstant(char c) {
		return getConstant(c, Character.class);
	}
	public Constant<Short> getConstant(short c) {
		return getConstant(c, Short.class);
	}
	public Constant<Integer> getConstant(int c) {
		return getConstant(c, Integer.class);
	}
	public Constant<Long> getConstant(long c) {
		return getConstant(c, Long.class);
	}
	public Constant<Float> getConstant(float c) {
		return getConstant(c, Float.class);
	}
	public Constant<Double> getConstant(double c) {
		return getConstant(c, Double.class);
	}
	public Constant<String> getConstant(String c) {
		return getConstant(checkNotNull(c), String.class);
	}
	@SuppressWarnings("unchecked")
	public Constant<Class<?>> getConstant(Class<?> c) {
		return (Constant<Class<?>>)(Constant)getConstant(checkNotNull(c), Class.class);
	}

	public <T> Constant<T> getConstant(T t, Class<T> klass) {
		return getConstant(checkNotNull(t)).as(klass);
	}

	public Constant<?> getNullConstant() {
		return getConstant((Object)null);
	}

	/**
	 * Gets the constant representing the given integer in the smallest constant
	 * type that represents it exactly.  (From smallest to largest, the types
	 * are boolean, byte, char, short, and int.)
	 *
	 * This method is intended for use when converting JVM-level constants,
	 * which are all represented as the int type.  Long, float and double
	 * constants are always distinguished.
	 * @param x the constant integer
	 * @return the constant of the smallest type representing the given integer
	 */
	public Constant<?> getSmallestIntConstant(int x) {
		if (x == 0)
			return getConstant(false);
		if (x == 1)
			return getConstant(true);
		if (Byte.MIN_VALUE <= x && x <= Byte.MAX_VALUE)
			return getConstant((byte)x);
		//The decision between char and short is ambiguous, as neither type
		//includes all values of the other.
		if (Character.MIN_VALUE <= x && x <= Character.MAX_VALUE)
			return getConstant((char)x);
		if (Short.MIN_VALUE <= x && x<= Short.MAX_VALUE)
			return getConstant((short)x);
		return getConstant(x);
	}

	@Override
	public Iterator<Constant<?>> iterator() {
		return constantsMap.values().iterator();
	}

	private Constant<?> getConstant(Object obj) {
		Constant<?> cst = constantsMap.get(obj);
		if (cst == null) {
			Type type;
			if (obj != null) {
				type = parent.types().getReferenceType(parent.getKlass(obj.getClass()));
				if (type instanceof WrapperType)
					type = ((WrapperType)type).unwrap();
			} else
				type = parent.types().getNullType();
			cst = new Constant<>(type, obj, parent);
			constantsMap.put(obj, cst);
		}
		return cst;
	}
}