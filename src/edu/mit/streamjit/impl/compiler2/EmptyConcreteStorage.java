package edu.mit.streamjit.impl.compiler2;

import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findVirtual;
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
	private static final MethodHandle READ = findVirtual(LOOKUP, "read");
	private static final MethodHandle WRITE = findVirtual(LOOKUP, "write");
	private static final MethodHandle ADJUST = findVirtual(LOOKUP, "adjust");
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
