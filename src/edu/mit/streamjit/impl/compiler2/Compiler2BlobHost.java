package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.util.Combinators;
import edu.mit.streamjit.util.MethodHandlePhaser;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleProxies;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.SwitchPoint;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.Phaser;

/**
 * The actual blob produced by a Compiler2.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/1/2013
 */
public class Compiler2BlobHost implements Blob {
	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle MAIN_LOOP, DO_INIT, DO_ADJUST, THROW_NEW_ASSERTION_ERROR;
	static {
		try {
			MAIN_LOOP = LOOKUP.findVirtual(Compiler2BlobHost.class, "mainLoop",
					MethodType.methodType(void.class, MethodHandle.class));
			DO_INIT = LOOKUP.findVirtual(Compiler2BlobHost.class, "doInit",
					MethodType.methodType(void.class));
			DO_ADJUST = LOOKUP.findVirtual(Compiler2BlobHost.class, "doAdjust",
					MethodType.methodType(void.class));

			MethodHandle newAssertionError = LOOKUP.findConstructor(AssertionError.class, MethodType.methodType(void.class, Object.class));
			MethodHandle throwAE = MethodHandles.throwException(void.class, AssertionError.class);
			THROW_NEW_ASSERTION_ERROR = MethodHandles.filterReturnValue(newAssertionError, throwAE);
		} catch (NoSuchMethodException | IllegalAccessException ex) {
			throw new AssertionError("Can't happen!", ex);
		}
	}
	private static final MethodHandle NOP = Combinators.nop();
	private static final MethodHandle MAIN_LOOP_NOP = MethodHandles.insertArguments(MAIN_LOOP, 1, NOP);

	/* provided by Compiler2 */
	private final ImmutableSet<Worker<?, ?>> workers;
	private final ImmutableSortedSet<Token> inputTokens, outputTokens;
	private final MethodHandle initCode;
	private final ImmutableList<MethodHandle> steadyStateCode;
	private final ImmutableMap<Token, Integer> tokenInitSchedule, tokenSteadyStateSchedule;
	private final ImmutableMap<Token, ConcreteStorage> tokenInitStorage, tokenSteadyStateStorage;
	private ImmutableList<Runnable> migrationInstructions;
	private final ImmutableList<MethodHandle> storageAdjusts;
	/* provided by the host */
	private ImmutableMap<Token, Buffer> buffers;
	private final ImmutableList<Runnable> coreCode;
	private final SwitchPoint sp1 = new SwitchPoint(), sp2 = new SwitchPoint();
	private final Phaser barrier;
	private volatile Runnable drainCallback;

	public Compiler2BlobHost(ImmutableSet<Worker<?, ?>> workers,
			ImmutableSortedSet<Token> inputTokens,
			ImmutableSortedSet<Token> outputTokens,
			MethodHandle initCode,
			ImmutableList<MethodHandle> steadyStateCode,
			ImmutableMap<Token, Integer> tokenInitSchedule,
			ImmutableMap<Token, Integer> tokenSteadyStateSchedule,
			ImmutableMap<Token, ConcreteStorage> tokenInitStorage,
			ImmutableMap<Token, ConcreteStorage> tokenSteadyStateStorage,
			ImmutableList<Runnable> migrationInstructions,
			ImmutableList<MethodHandle> storageAdjusts) {
		this.workers = workers;
		this.inputTokens = inputTokens;
		this.outputTokens = outputTokens;
		this.initCode = initCode;
		this.steadyStateCode = steadyStateCode;
		this.tokenInitSchedule = tokenInitSchedule;
		this.tokenSteadyStateSchedule = tokenSteadyStateSchedule;
		this.tokenInitStorage = tokenInitStorage;
		this.tokenSteadyStateStorage = tokenSteadyStateStorage;
		this.migrationInstructions = migrationInstructions;
		this.storageAdjusts = storageAdjusts;

		MethodHandle mainLoop = MAIN_LOOP.bindTo(this),
				doInit = DO_INIT.bindTo(this),
				doAdjust = DO_ADJUST.bindTo(this),
				mainLoopNop = MAIN_LOOP_NOP.bindTo(this);
		ImmutableList.Builder<Runnable> coreCodeBuilder = ImmutableList.builder();
		for (MethodHandle ssc : this.steadyStateCode) {
			MethodHandle code = sp1.guardWithTest(mainLoopNop, sp2.guardWithTest(mainLoop.bindTo(ssc), NOP));
			coreCodeBuilder.add(MethodHandleProxies.asInterfaceInstance(Runnable.class, code));
		}
		this.coreCode = coreCodeBuilder.build();
		MethodHandle throwAE = THROW_NEW_ASSERTION_ERROR.bindTo("Can't happen! Barrier action reached after draining?");
		this.barrier = new MethodHandlePhaser(sp1.guardWithTest(doInit, sp2.guardWithTest(doAdjust, throwAE)), coreCode.size());
	}

	@Override
	public Set<Worker<?, ?>> getWorkers() {
		return workers;
	}

	@Override
	public Set<Token> getInputs() {
		return inputTokens;
	}

	@Override
	public Set<Token> getOutputs() {
		return outputTokens;
	}

	@Override
	public int getMinimumBufferCapacity(Token token) {
		if (inputTokens.contains(token))
			return Math.max(tokenInitSchedule.get(token), tokenSteadyStateSchedule.get(token));
		if (outputTokens.contains(token))
			return 1;
		throw new IllegalArgumentException(token.toString()+" not an input or output of this blob");
	}

	@Override
	public void installBuffers(Map<Token, Buffer> buffers) {
		if (this.buffers != null)
			throw new IllegalStateException("installBuffers called more than once");
		ImmutableMap.Builder<Token, Buffer> builder = ImmutableMap.builder();
		for (Token t : Sets.union(inputTokens, outputTokens)) {
			Buffer b = buffers.get(t);
			if (b == null)
				throw new IllegalArgumentException("no buffer for token "+t);
			builder.put(t, b);
		}
		this.buffers = builder.build();
	}

	@Override
	public int getCoreCount() {
		return coreCode.size();
	}

	@Override
	public Runnable getCoreCode(int core) {
		return coreCode.get(core);
	}

	@Override
	public void drain(Runnable callback) {
		drainCallback = callback;
	}

	@Override
	public DrainData getDrainData() {
		//TODO
		return null;
	}

	private void mainLoop(MethodHandle coreCode) throws Throwable {
		try {
			coreCode.invokeExact();
		} catch (Throwable ex) {
			barrier.forceTermination();
			throw ex;
		}
		barrier.arriveAndAwaitAdvance();
	}

	private void doInit() throws Throwable {
		//Fill inputs, or drain.  We read into the map, only committing into
		//ConcreteStorage after all reads complete.
		Map<Token, Object[]> initData = new HashMap<>();
		for (Token t : inputTokens) {
			int items = tokenInitSchedule.get(t);
			Buffer b = buffers.get(t);
			Object[] data = new Object[items];
			while (!b.readAll(data))
				if (isDraining()) {
					doDrain(/* TODO liveness */);
					return;
				}
			initData.put(t, data);
		}
		for (Token t : inputTokens) {
			Object[] data = initData.get(t);
			ConcreteStorage storage = tokenInitStorage.get(t);
			MethodHandle mh = storage.writeHandle();
			for (int i = 0; i < data.length; ++i)
				mh.invokeExact(i, data[i]);
		}

		try {
			initCode.invoke();
		} catch (Throwable ex) {
			barrier.forceTermination();
			throw ex;
		}

		//Write outputs, if any.
		for (Token t : outputTokens) {
			int items = tokenInitSchedule.get(t);
			if (items == 0)
				continue;
			ConcreteStorage storage = tokenInitStorage.get(t);
			MethodHandle mh = storage.readHandle();
			Buffer b = buffers.get(t);
			Object[] data = new Object[items];
			for (int i = 0; i < data.length; ++i)
				data[i] = mh.invokeExact(i);
			int written = 0;
			while (written != data.length)
				written += b.write(data, written, data.length-written);
		}

		for (Runnable r : migrationInstructions)
			r.run();
		migrationInstructions = null;

		SwitchPoint.invalidateAll(new SwitchPoint[]{sp1});
	}

	private void doAdjust() throws Throwable {
		//Write outputs.
		for (Token t : outputTokens) {
			int items = tokenSteadyStateSchedule.get(t);
			if (items == 0)
				continue;
			ConcreteStorage storage = tokenSteadyStateStorage.get(t);
			MethodHandle mh = storage.readHandle();
			Buffer b = buffers.get(t);
			Object[] data = new Object[items];
			for (int i = 0; i < data.length; ++i)
				data[i] = mh.invokeExact(i);
			int written = 0;
			while (written != data.length)
				written += b.write(data, written, data.length-written);
		}

		for (MethodHandle h : storageAdjusts)
			h.invokeExact();

		//Fill inputs, or drain.
		Map<Token, Object[]> inputData = new HashMap<>();
		for (Token t : inputTokens) {
			int items = tokenSteadyStateSchedule.get(t);
			Buffer b = buffers.get(t);
			Object[] data = new Object[items];
			while (!b.readAll(data))
				if (isDraining()) {
					doDrain(/* TODO liveness */);
					return;
				}
			inputData.put(t, data);
		}
		for (Token t : inputTokens) {
			Object[] data = inputData.get(t);
			ConcreteStorage storage = tokenSteadyStateStorage.get(t);
			MethodHandle mh = storage.writeHandle();
			for (int i = 0; i < data.length; ++i)
				mh.invokeExact(i, data[i]);
		}
	}

	private void doDrain() {
		//TODO: actually implement this (live items in ConcreteStorage + uncommitted reads)
		drainCallback.run();
	}

	private boolean isDraining() {
		return drainCallback != null;
	}
}
