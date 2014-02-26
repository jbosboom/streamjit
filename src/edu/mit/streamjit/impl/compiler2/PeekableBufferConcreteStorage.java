package edu.mit.streamjit.impl.compiler2;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.PeekableBuffer;
import edu.mit.streamjit.util.Combinators;
import edu.mit.streamjit.util.LookupUtils;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;

/**
 * A read-only ConcreteStorage implementation wrapping an PeekableBuffer.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 2/18/2014
 */
public final class PeekableBufferConcreteStorage implements ConcreteStorage {
	private static final MethodHandle IRB_PEEK = LookupUtils.findVirtual(MethodHandles.publicLookup(), PeekableBuffer.class, "peek", Object.class, int.class);
	private static final MethodHandle IRB_CONSUME = LookupUtils.findVirtual(MethodHandles.publicLookup(), PeekableBuffer.class, "consume", void.class, int.class);
	private final Class<?> type;
	private final int throughput, minReadIndex;
	private final PeekableBuffer buffer;
	private final MethodHandle readHandle, adjustHandle;
	public PeekableBufferConcreteStorage(Class<?> type, int throughput, int minReadIndex, PeekableBuffer buffer) {
		this.type = type;
		this.throughput = throughput;
		this.minReadIndex = minReadIndex;
		this.buffer = buffer;
		this.readHandle = MethodHandles.filterArguments(IRB_PEEK.bindTo(buffer), 0, Combinators.sub(MethodHandles.identity(int.class), minReadIndex));
		this.adjustHandle = MethodHandles.insertArguments(IRB_CONSUME, 0, buffer, throughput);
	}

	@Override
	public Class<?> type() {
		return type;
	}

	@Override
	public Object read(int index) {
		try {
			return readHandle.invoke(index);
		} catch (Throwable ex) {
			throw new AssertionError(String.format("%s.read(%d, %s)", this, index), ex);
		}
	}

	@Override
	public void write(int index, Object data) {
		throw new AssertionError(String.format("read-only! %s.write(%d, %s)", this, index, data));
	}

	@Override
	public void adjust() {
		try {
			adjustHandle.invokeExact();
		} catch (Throwable ex) {
			throw new AssertionError(String.format("%s.adjust()", this), ex);
		}
	}

	@Override
	public void sync() {
	}

	@Override
	public MethodHandle readHandle() {
		return readHandle;
	}

	@Override
	public MethodHandle writeHandle() {
		throw new UnsupportedOperationException("read-only");
	}

	@Override
	public MethodHandle adjustHandle() {
		return adjustHandle;
	}

	public PeekableBuffer buffer() {
		return buffer;
	}

	public int minReadIndex() {
		return minReadIndex;
	}

	public static StorageFactory factory(final Map<Token, PeekableBuffer> buffers) {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				assert buffers.containsKey(storage.id()) : storage.id()+" not in "+buffers;
				//Hack: we don't have the throughput when making init storage,
				//but we don't need it either.
				int throughput;
				try {
					throughput = storage.throughput();
				} catch (IllegalStateException ignored) {
					throughput = Integer.MIN_VALUE;
				}
				ImmutableSet<ActorGroup> relevantGroups = ImmutableSet.<ActorGroup>builder()
						.addAll(storage.upstreamGroups()).addAll(storage.downstreamGroups()).build();
				int minReadIndex = storage.readIndices(Maps.asMap(relevantGroups, new Function<ActorGroup, Integer>() {
					@Override
					public Integer apply(ActorGroup input) {
						return 1;
					}
				})).first();
				return new PeekableBufferConcreteStorage(storage.type(), throughput, minReadIndex, buffers.get(storage.id()));
			}
		};
	}
}
