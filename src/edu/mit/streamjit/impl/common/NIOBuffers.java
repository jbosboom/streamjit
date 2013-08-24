package edu.mit.streamjit.impl.common;

import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.impl.blob.AbstractReadOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

/**
 * Creates Buffer instances wrapping java.nio.Buffers of a particular type.
 * <p/>
 * This class uses code generation to work around the fact that NIO provides
 * ByteBuffer, IntBuffer etc. rather than Buffer<Byte>, Buffer<Integer> etc.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/23/2013
 */
public final class NIOBuffers {
	private NIOBuffers() {}

	//<editor-fold defaultstate="collapsed" desc="Code generator">
	private static final class CodeGenRecord {
		private final Class<?> buffer, wrapper;
		private final String byteBufferToTypeBuffer;
		private CodeGenRecord(Class<?> buffer, Class<?> wrapper) {
			this.buffer = buffer;
			this.wrapper = wrapper;
			this.byteBufferToTypeBuffer = buffer == ByteBuffer.class ? "" : ".as"+buffer.getSimpleName()+"()";
		}
	}

	private static final ImmutableList<CodeGenRecord> RECORDS = ImmutableList.of(
			new CodeGenRecord(ByteBuffer.class, Byte.class),
			new CodeGenRecord(ShortBuffer.class, Short.class),
			new CodeGenRecord(CharBuffer.class, Character.class),
			new CodeGenRecord(IntBuffer.class, Integer.class),
			new CodeGenRecord(LongBuffer.class, Long.class),
			new CodeGenRecord(FloatBuffer.class, Float.class),
			new CodeGenRecord(DoubleBuffer.class, Double.class)
			);

	private static final String BUFFER_TEMPLATE =
			"	private static final class $BUFFER$Buffer extends AbstractReadOnlyBuffer {\n"+
			"		private final $BUFFER$ buffer;\n"+
			"		private $BUFFER$Buffer($BUFFER$ buffer) {\n"+
			"			this.buffer = buffer;\n"+
			"		}\n"+
			"		@Override\n"+
			"		public Object read() {\n"+
			"			try {\n"+
			"				return buffer.get();\n"+
			"			} catch (BufferUnderflowException ex) {\n"+
			"				return null;\n"+
			"			}\n"+
			"		}\n"+
			"		@Override\n"+
			"		public int size() {\n"+
			"			return buffer.remaining();\n"+
			"		}\n"+
			"	}";
	private static final String WRAP_HEADER =
			"	public static Buffer wrap(ByteBuffer buffer, Class<?> type) {\n";
	private static final String WRAP_PER_RECORD =
			"		if (type == $WRAPPER$.class) return new $BUFFER$Buffer(buffer$TOTYPEBUFFER$);\n";
	private static final String WRAP_FOOTER =
			"		throw new AssertionError(\"not a wrapper type: \"+type);\n"+
			"	}";

	private static String replace(String string, CodeGenRecord record) {
		return string.replace("$BUFFER$", record.buffer.getSimpleName())
				.replace("$WRAPPER$", record.wrapper.getSimpleName())
				.replace("$TOTYPEBUFFER$", record.byteBufferToTypeBuffer);
	}

	public static void main(String[] args) {
		System.out.println("	//<editor-fold defaultstate=\"collapsed\" desc=\"Generated code\">");
		System.out.print(WRAP_HEADER);
		for (CodeGenRecord r : RECORDS)
			System.out.print(replace(WRAP_PER_RECORD, r));
		System.out.println(WRAP_FOOTER);
		for (CodeGenRecord r : RECORDS)
			System.out.println(replace(BUFFER_TEMPLATE, r));
		System.out.println("	//</editor-fold>");
		System.out.flush();
	}
	//</editor-fold>

	//<editor-fold defaultstate="collapsed" desc="Generated code">
	public static Buffer wrap(ByteBuffer buffer, Class<?> type) {
		if (type == Byte.class) return new ByteBufferBuffer(buffer);
		if (type == Short.class) return new ShortBufferBuffer(buffer.asShortBuffer());
		if (type == Character.class) return new CharBufferBuffer(buffer.asCharBuffer());
		if (type == Integer.class) return new IntBufferBuffer(buffer.asIntBuffer());
		if (type == Long.class) return new LongBufferBuffer(buffer.asLongBuffer());
		if (type == Float.class) return new FloatBufferBuffer(buffer.asFloatBuffer());
		if (type == Double.class) return new DoubleBufferBuffer(buffer.asDoubleBuffer());
		throw new AssertionError("not a wrapper type: "+type);
	}
	private static final class ByteBufferBuffer extends AbstractReadOnlyBuffer {
		private final ByteBuffer buffer;
		private ByteBufferBuffer(ByteBuffer buffer) {
			this.buffer = buffer;
		}
		@Override
		public Object read() {
			try {
				return buffer.get();
			} catch (BufferUnderflowException ex) {
				return null;
			}
		}
		@Override
		public int size() {
			return buffer.remaining();
		}
	}
	private static final class ShortBufferBuffer extends AbstractReadOnlyBuffer {
		private final ShortBuffer buffer;
		private ShortBufferBuffer(ShortBuffer buffer) {
			this.buffer = buffer;
		}
		@Override
		public Object read() {
			try {
				return buffer.get();
			} catch (BufferUnderflowException ex) {
				return null;
			}
		}
		@Override
		public int size() {
			return buffer.remaining();
		}
	}
	private static final class CharBufferBuffer extends AbstractReadOnlyBuffer {
		private final CharBuffer buffer;
		private CharBufferBuffer(CharBuffer buffer) {
			this.buffer = buffer;
		}
		@Override
		public Object read() {
			try {
				return buffer.get();
			} catch (BufferUnderflowException ex) {
				return null;
			}
		}
		@Override
		public int size() {
			return buffer.remaining();
		}
	}
	private static final class IntBufferBuffer extends AbstractReadOnlyBuffer {
		private final IntBuffer buffer;
		private IntBufferBuffer(IntBuffer buffer) {
			this.buffer = buffer;
		}
		@Override
		public Object read() {
			try {
				return buffer.get();
			} catch (BufferUnderflowException ex) {
				return null;
			}
		}
		@Override
		public int size() {
			return buffer.remaining();
		}
	}
	private static final class LongBufferBuffer extends AbstractReadOnlyBuffer {
		private final LongBuffer buffer;
		private LongBufferBuffer(LongBuffer buffer) {
			this.buffer = buffer;
		}
		@Override
		public Object read() {
			try {
				return buffer.get();
			} catch (BufferUnderflowException ex) {
				return null;
			}
		}
		@Override
		public int size() {
			return buffer.remaining();
		}
	}
	private static final class FloatBufferBuffer extends AbstractReadOnlyBuffer {
		private final FloatBuffer buffer;
		private FloatBufferBuffer(FloatBuffer buffer) {
			this.buffer = buffer;
		}
		@Override
		public Object read() {
			try {
				return buffer.get();
			} catch (BufferUnderflowException ex) {
				return null;
			}
		}
		@Override
		public int size() {
			return buffer.remaining();
		}
	}
	private static final class DoubleBufferBuffer extends AbstractReadOnlyBuffer {
		private final DoubleBuffer buffer;
		private DoubleBufferBuffer(DoubleBuffer buffer) {
			this.buffer = buffer;
		}
		@Override
		public Object read() {
			try {
				return buffer.get();
			} catch (BufferUnderflowException ex) {
				return null;
			}
		}
		@Override
		public int size() {
			return buffer.remaining();
		}
	}
	//</editor-fold>
}
