package edu.mit.streamjit.impl.common;

import com.google.common.reflect.Reflection;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.impl.blob.Buffer;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/17/2013
 */
public abstract class OutputBufferFactory {
	public abstract Buffer createWritableBuffer(int writerMinSize);

	public static MethodHandles.Lookup OUTPUT_LOOKUP;
	private static final class InputHolder {
		private static final MethodHandle getOutputBufferFactory;
		static {
			Reflection.initialize(Output.class);
			assert OUTPUT_LOOKUP != null;
			try {
				getOutputBufferFactory = OUTPUT_LOOKUP.findGetter(Output.class, "output", OutputBufferFactory.class);
			} catch (NoSuchFieldException | IllegalAccessException ex) {
				throw new RuntimeException(ex);
			}
		}
	}

	public static OutputBufferFactory unwrap(Output<?> input) {
		try {
			return (OutputBufferFactory)InputHolder.getOutputBufferFactory.invokeExact(input);
		} catch (Throwable ex) {
			throw new RuntimeException(ex);
		}
	}
}
