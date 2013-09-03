package edu.mit.streamjit.impl.common;

import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.impl.blob.AbstractReadOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.util.Template;
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
		private final String buffer, wrapper;
		private final String toTypeBuffer;
		private CodeGenRecord(Class<?> buffer, Class<?> wrapper) {
			this.buffer = buffer.getSimpleName();
			this.wrapper = wrapper.getSimpleName();
			this.toTypeBuffer = buffer == ByteBuffer.class ? "" : ".as"+buffer.getSimpleName()+"()";
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
			"	private static final class ${buffer}Buffer extends AbstractReadOnlyBuffer {\n"+
			"		private final ${buffer} buffer;\n"+
			"		private ${buffer}Buffer(${buffer} buffer) {\n"+
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
			"	}\n";
	private static final String WRAP_HEADER =
			"	public static Buffer wrap(ByteBuffer buffer, Class<?> type) {\n";
	private static final String WRAP_PER_RECORD =
			"		if (type == ${wrapper}.class) return new ${buffer}Buffer(buffer${toTypeBuffer});\n";
	private static final String WRAP_FOOTER =
			"		throw new AssertionError(\"not a wrapper type: \"+type);\n"+
			"	}\n";

	public static void main(String[] args) {
		StringBuffer sb = new StringBuffer();
		sb.append("	//<editor-fold defaultstate=\"collapsed\" desc=\"Generated code\">\n");
		sb.append(WRAP_HEADER);
		Template ifReturn = new Template(WRAP_PER_RECORD);
		ifReturn.replaceReflect(RECORDS, sb);
		sb.append(WRAP_FOOTER);
		Template bufferClass = new Template(BUFFER_TEMPLATE);
		bufferClass.replaceReflect(RECORDS, sb);
		sb.append("	//</editor-fold>\n");
		System.out.println(sb.toString());
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
