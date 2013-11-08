package edu.mit.streamjit.util.bytecode.types;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ArrayTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.google.common.primitives.Primitives;
import edu.mit.streamjit.util.bytecode.Klass;
import java.util.Arrays;
import java.util.List;
import org.objectweb.asm.Opcodes;

/**
 * A primitive type.  (Note that void is not considered a primitive type, even
 * though void.class.isPrimitive() returns true.)
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/7/2013
 */
public final class PrimitiveType extends RegularType {
	public PrimitiveType(Klass klass) {
		super(klass);
		Class<?> backing = klass.getBackingClass();
		checkArgument(backing != null && backing.isPrimitive() && !backing.equals(void.class),
				"not a primitive type: %s", klass);
	}

	public WrapperType wrap() {
		//PrimitiveTypes are always backed by Classes.
		return getTypeFactory().getWrapperType(getModule().getKlass(Primitives.wrap(getKlass().getBackingClass())));
	}

	/**
	 * PrimitiveTypes are subtypes of the PrimitiveTypes to which they can be
	 * converted without cast instructions (at the JVM level), and no other
	 * types.
	 *
	 * Note that some conversions that do not lose information about the value
	 * nevertheless require cast instructions.
	 * @param other {@inheritDoc}
	 * @return {@inheritDoc}
	 */
	@Override
	public boolean isSubtypeOf(Type other) {
		if (!(other instanceof PrimitiveType))
			return false;
		return getCastOpcode((PrimitiveType)other).isEmpty();
	}

	private final ImmutableMap<String, String> DESCRIPTORS = ImmutableMap.<String, String>builder()
			.put(boolean.class.getName(), "Z")
			.put(byte.class.getName(), "B")
			.put(char.class.getName(), "C")
			.put(short.class.getName(), "S")
			.put(int.class.getName(), "I")
			.put(long.class.getName(), "J")
			.put(float.class.getName(), "F")
			.put(double.class.getName(), "D")
			.build();
	@Override
	public String getDescriptor() {
		return DESCRIPTORS.get(getKlass().getBackingClass().getName());
	}

	@Override
	public int getCategory() {
		if (equals(getTypeFactory().getType(long.class)) || equals(getTypeFactory().getType(double.class)))
			return 2;
		return super.getCategory();
	}

	private static final List<String> PRIMITIVE_TYPE_NAMES = Arrays.asList(
			boolean.class.getName(), byte.class.getName(),
			char.class.getName(), short.class.getName(),
			int.class.getName(), long.class.getName(),
			float.class.getName(), double.class.getName()
			);
	private static final Table<String, String, ImmutableList<Integer>> CAST_OPCODE_TABLE =
			ArrayTable.create(PRIMITIVE_TYPE_NAMES, PRIMITIVE_TYPE_NAMES);
	static {
		//booleans are encoded as 0 or 1, so they fit in all the non-long integer types.
		CAST_OPCODE_TABLE.put(boolean.class.getName(), long.class.getName(), ImmutableList.of(Opcodes.I2L));
		CAST_OPCODE_TABLE.put(boolean.class.getName(), float.class.getName(), ImmutableList.of(Opcodes.I2F));
		CAST_OPCODE_TABLE.put(boolean.class.getName(), double.class.getName(), ImmutableList.of(Opcodes.I2D));

		CAST_OPCODE_TABLE.put(byte.class.getName(), char.class.getName(), ImmutableList.of(Opcodes.I2C));
		CAST_OPCODE_TABLE.put(byte.class.getName(), long.class.getName(), ImmutableList.of(Opcodes.I2L));
		CAST_OPCODE_TABLE.put(byte.class.getName(), float.class.getName(), ImmutableList.of(Opcodes.I2F));
		CAST_OPCODE_TABLE.put(byte.class.getName(), double.class.getName(), ImmutableList.of(Opcodes.I2D));

		CAST_OPCODE_TABLE.put(char.class.getName(), byte.class.getName(), ImmutableList.of(Opcodes.I2B));
		CAST_OPCODE_TABLE.put(char.class.getName(), short.class.getName(), ImmutableList.of(Opcodes.I2S));
		CAST_OPCODE_TABLE.put(char.class.getName(), long.class.getName(), ImmutableList.of(Opcodes.I2L));
		CAST_OPCODE_TABLE.put(char.class.getName(), float.class.getName(), ImmutableList.of(Opcodes.I2F));
		CAST_OPCODE_TABLE.put(char.class.getName(), double.class.getName(), ImmutableList.of(Opcodes.I2D));

		CAST_OPCODE_TABLE.put(short.class.getName(), byte.class.getName(), ImmutableList.of(Opcodes.I2B));
		CAST_OPCODE_TABLE.put(short.class.getName(), char.class.getName(), ImmutableList.of(Opcodes.I2C));
		CAST_OPCODE_TABLE.put(short.class.getName(), long.class.getName(), ImmutableList.of(Opcodes.I2L));
		CAST_OPCODE_TABLE.put(short.class.getName(), float.class.getName(), ImmutableList.of(Opcodes.I2F));
		CAST_OPCODE_TABLE.put(short.class.getName(), double.class.getName(), ImmutableList.of(Opcodes.I2D));

		CAST_OPCODE_TABLE.put(int.class.getName(), byte.class.getName(), ImmutableList.of(Opcodes.I2B));
		CAST_OPCODE_TABLE.put(int.class.getName(), char.class.getName(), ImmutableList.of(Opcodes.I2C));
		CAST_OPCODE_TABLE.put(int.class.getName(), short.class.getName(), ImmutableList.of(Opcodes.I2S));
		CAST_OPCODE_TABLE.put(int.class.getName(), long.class.getName(), ImmutableList.of(Opcodes.I2L));
		CAST_OPCODE_TABLE.put(int.class.getName(), float.class.getName(), ImmutableList.of(Opcodes.I2F));
		CAST_OPCODE_TABLE.put(int.class.getName(), double.class.getName(), ImmutableList.of(Opcodes.I2D));

		CAST_OPCODE_TABLE.put(long.class.getName(), byte.class.getName(), ImmutableList.of(Opcodes.L2I, Opcodes.I2B));
		CAST_OPCODE_TABLE.put(long.class.getName(), char.class.getName(), ImmutableList.of(Opcodes.L2I, Opcodes.I2C));
		CAST_OPCODE_TABLE.put(long.class.getName(), short.class.getName(), ImmutableList.of(Opcodes.L2I, Opcodes.I2S));
		CAST_OPCODE_TABLE.put(long.class.getName(), int.class.getName(), ImmutableList.of(Opcodes.L2I));
		CAST_OPCODE_TABLE.put(long.class.getName(), float.class.getName(), ImmutableList.of(Opcodes.L2F));
		CAST_OPCODE_TABLE.put(long.class.getName(), double.class.getName(), ImmutableList.of(Opcodes.L2D));

		CAST_OPCODE_TABLE.put(float.class.getName(), byte.class.getName(), ImmutableList.of(Opcodes.F2I, Opcodes.I2B));
		CAST_OPCODE_TABLE.put(float.class.getName(), char.class.getName(), ImmutableList.of(Opcodes.F2I, Opcodes.I2C));
		CAST_OPCODE_TABLE.put(float.class.getName(), short.class.getName(), ImmutableList.of(Opcodes.F2I, Opcodes.I2S));
		CAST_OPCODE_TABLE.put(float.class.getName(), int.class.getName(), ImmutableList.of(Opcodes.F2I));
		CAST_OPCODE_TABLE.put(float.class.getName(), long.class.getName(), ImmutableList.of(Opcodes.F2L));
		CAST_OPCODE_TABLE.put(float.class.getName(), double.class.getName(), ImmutableList.of(Opcodes.F2D));

		CAST_OPCODE_TABLE.put(double.class.getName(), byte.class.getName(), ImmutableList.of(Opcodes.D2I, Opcodes.I2B));
		CAST_OPCODE_TABLE.put(double.class.getName(), char.class.getName(), ImmutableList.of(Opcodes.D2I, Opcodes.I2C));
		CAST_OPCODE_TABLE.put(double.class.getName(), short.class.getName(), ImmutableList.of(Opcodes.D2I, Opcodes.I2S));
		CAST_OPCODE_TABLE.put(double.class.getName(), int.class.getName(), ImmutableList.of(Opcodes.D2I));
		CAST_OPCODE_TABLE.put(double.class.getName(), long.class.getName(), ImmutableList.of(Opcodes.D2L));
		CAST_OPCODE_TABLE.put(double.class.getName(), float.class.getName(), ImmutableList.of(Opcodes.D2F));

		//Conversion of any non-boolean type to boolean requires logic; mark
		//those conversions with a bogus opcode.
		CAST_OPCODE_TABLE.put(byte.class.getName(), boolean.class.getName(), ImmutableList.of(Opcodes.NOP));
		CAST_OPCODE_TABLE.put(char.class.getName(), boolean.class.getName(), ImmutableList.of(Opcodes.NOP));
		CAST_OPCODE_TABLE.put(short.class.getName(), boolean.class.getName(), ImmutableList.of(Opcodes.NOP));
		CAST_OPCODE_TABLE.put(int.class.getName(), boolean.class.getName(), ImmutableList.of(Opcodes.NOP));
		CAST_OPCODE_TABLE.put(long.class.getName(), boolean.class.getName(), ImmutableList.of(Opcodes.NOP));
		CAST_OPCODE_TABLE.put(float.class.getName(), boolean.class.getName(), ImmutableList.of(Opcodes.NOP));
		CAST_OPCODE_TABLE.put(double.class.getName(), boolean.class.getName(), ImmutableList.of(Opcodes.NOP));

		//Anything that doesn't yet have a value gets an empty list.
		for (String x : PRIMITIVE_TYPE_NAMES)
			for (String y : PRIMITIVE_TYPE_NAMES)
				if (CAST_OPCODE_TABLE.get(x, y) == null)
					CAST_OPCODE_TABLE.put(x, y, ImmutableList.<Integer>of());
	}

	/**
	 * Returns a list of the opcodes required to convert from this type to the
	 * given type, or an empty list if no cast instructions are required.
	 * @param other the type to convert to
	 * @return a list of the opcodes required to convert from this type to the
	 * given type, or an empty list if no cast instructions are required
	 */
	public ImmutableList<Integer> getCastOpcode(PrimitiveType other) {
		return CAST_OPCODE_TABLE.get(getKlass().getBackingClass().getName(), other.getKlass().getBackingClass().getName());
	}
}
