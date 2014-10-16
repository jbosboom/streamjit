package edu.mit.streamjit.impl.compiler2;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import edu.mit.streamjit.util.bytecode.methodhandles.Combinators;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findGetter;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findVirtual;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.Map;

/**
 * A ConcreteStorage backed by double-buffered storage.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/10/2013
 */
public class DoubleArrayConcreteStorage implements ConcreteStorage {
	private static final Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle ADJUST = findVirtual(LOOKUP, "adjust");
	private static final MethodHandle STATE_GETTER = findGetter(LOOKUP, "state");
	private final Arrayish readArray, writeArray;
	/**
	 * If true, read from readArray, else writeArray, and resp. for writes.
	 */
	private boolean state = true;
	private final int capacity, throughput, readOffset;
	private final MethodHandle readHandle, writeHandle, adjustHandle;
	public DoubleArrayConcreteStorage(Arrayish.Factory arrayFactory, Storage s) {
		this.capacity = s.steadyStateCapacity();
		assert capacity > 0 : s + " has capacity "+capacity;
		this.throughput = s.throughput();
		assert capacity == 2*throughput : "can't double buffer "+s;
		this.readArray = arrayFactory.make(s.type(), throughput);
		this.writeArray = arrayFactory.make(s.type(), throughput);

		ImmutableSet<ActorGroup> relevantGroups = ImmutableSet.<ActorGroup>builder().addAll(s.upstreamGroups()).addAll(s.downstreamGroups()).build();
		Map<ActorGroup, Integer> oneMap = Maps.asMap(relevantGroups, new Function<ActorGroup, Integer>() {
			@Override
			public Integer apply(ActorGroup input) {
				return 1;
			}
		});
		this.readOffset = s.readIndices(oneMap).first();
		int writeOffset = s.writeIndices(oneMap).first();

		MethodHandle stateGetter = STATE_GETTER.bindTo(this);
		this.readHandle = MethodHandles.filterArguments(MethodHandles.guardWithTest(stateGetter, readArray.get(), writeArray.get()),
				0, Combinators.adder(-readOffset));
		this.writeHandle = MethodHandles.filterArguments(MethodHandles.guardWithTest(stateGetter, writeArray.set(), readArray.set()),
				0, Combinators.adder(-writeOffset));
		this.adjustHandle = ADJUST.bindTo(this);
	}

	@Override
	public Class<?> type() {
		return readArray.type();
	}

	@Override
	public void write(int index, Object data) {
		try {
			//Pretend the read and write arrays are contiguous.
			index -= readOffset;
			if (index < throughput)
				(state ? readArray : writeArray).set().invoke(index, data);
			else
				(state ? writeArray : readArray).set().invoke(index-throughput, data);
		} catch (Throwable ex) {
			throw new AssertionError(String.format("%s.write(%d, %s)", this, index, data), ex);
		}
	}

	@Override
	public void adjust() {
		//state != state doesn't work, heh.
		state = !state;
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
		return writeHandle;
	}

	@Override
	public MethodHandle adjustHandle() {
		return adjustHandle;
	}

	public static StorageFactory factory() {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				if (storage.steadyStateCapacity() == 0)
					return new EmptyConcreteStorage(storage);
				return new DoubleArrayConcreteStorage(Arrayish.ArrayArrayish.factory(), storage);
			}
		};
	}
}
