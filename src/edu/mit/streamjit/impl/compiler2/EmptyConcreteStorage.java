package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.util.LookupUtils;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

/**
 * An empty ConcreteStorage that cannot be read or written.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/15/2014
 */
public final class EmptyConcreteStorage implements ConcreteStorage {
	private final Class<?> type;
	private final String string;
	public EmptyConcreteStorage(Storage s) {
		this.type = s.type();
		this.string = s.toString();
	}
	@Override
	public Class<?> type() {
		return type;
	}
	@Override
	public Object read(int index) {
		throw new UnsupportedOperationException("reading from empty storage "+string);
	}
	@Override
	public void write(int index, Object data) {
		throw new UnsupportedOperationException("writing to empty storage "+string);
	}
	@Override
	public void adjust() {
	}
	@Override
	public void sync() {
	}
	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle READ = LookupUtils.findVirtual(LOOKUP, EmptyConcreteStorage.class, "read", Object.class, int.class);
	private static final MethodHandle WRITE = LookupUtils.findVirtual(LOOKUP, EmptyConcreteStorage.class, "write", void.class, int.class, Object.class);
	private static final MethodHandle ADJUST = LookupUtils.findVirtual(LOOKUP, EmptyConcreteStorage.class, "adjust", void.class);
	@Override
	public MethodHandle readHandle() {
		return READ.bindTo(this);
	}
	@Override
	public MethodHandle writeHandle() {
		return WRITE.bindTo(this);
	}
	@Override
	public MethodHandle adjustHandle() {
		return ADJUST.bindTo(this);
	}
	@Override
	public String toString() {
		return getClass().getSimpleName()+string;
	}
}
