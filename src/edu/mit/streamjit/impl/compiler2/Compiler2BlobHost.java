package edu.mit.streamjit.impl.compiler2;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.interp.Interpreter;
import edu.mit.streamjit.util.CollectionUtils;
import edu.mit.streamjit.util.Combinators;
import edu.mit.streamjit.util.MethodHandlePhaser;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleProxies;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.SwitchPoint;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;

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
	private final ImmutableList<MethodHandle> storageAdjusts;
	/**
	 * Instructions to load items for the init schedule.  unload() will
	 * unload all items, as the init schedule only runs if all reads can
	 * be satisfied.
	 */
	public ImmutableList<ReadInstruction> initReadInstructions;
	/**
	 * Instructions to write output from the init schedule.
	 */
	public ImmutableList<WriteInstruction> initWriteInstructions;
	/**
	 * Instructions to move items from init storage to steady-state storage.
	 */
	public ImmutableList<Runnable> migrationInstructions;
	/**
	 * Instructions to load items for the steady-state schedule.  unload()
	 * will only unload items loaded by load(); the drain instructions will
	 * retrieve any unconsumed items in the storage.
	 */
	public final ImmutableList<ReadInstruction> readInstructions;
	/**
	 * Instructions to write output from the steady-state schedule.
	 */
	public final ImmutableList<WriteInstruction> writeInstructions;
	/**
	 * Instructions to extract items from steady-state storage for transfer
	 * to a DrainData object.  For input storage, this only extracts
	 * unconsumed items (items at live indices except the last throughput
	 * indices, which are covered by the read instructions' unload()).
	 */
	public final ImmutableList<DrainInstruction> drainInstructions;
	/* provided by the host */
	private final ImmutableMap<Token, Integer> minimumBufferCapacity;
	private ImmutableMap<Token, Buffer> buffers;
	private final ImmutableList<Runnable> coreCode;
	private final SwitchPoint sp1 = new SwitchPoint(), sp2 = new SwitchPoint();
	private final Phaser barrier;
	private volatile Runnable drainCallback;
	private volatile DrainData drainData;

	public Compiler2BlobHost(ImmutableSet<Worker<?, ?>> workers,
			ImmutableSortedSet<Token> inputTokens,
			ImmutableSortedSet<Token> outputTokens,
			MethodHandle initCode,
			ImmutableList<MethodHandle> steadyStateCode,
			ImmutableList<MethodHandle> storageAdjusts,
			List<ReadInstruction> initReadInstructions,
			List<WriteInstruction> initWriteInstructions,
			List<Runnable> migrationInstructions,
			List<ReadInstruction> readInstructions,
			List<WriteInstruction> writeInstructions,
			List<DrainInstruction> drainInstructions) {
		this.workers = workers;
		this.inputTokens = inputTokens;
		this.outputTokens = outputTokens;
		this.initCode = initCode;
		this.steadyStateCode = steadyStateCode;
		this.storageAdjusts = storageAdjusts;
		this.initReadInstructions = ImmutableList.copyOf(initReadInstructions);
		this.initWriteInstructions = ImmutableList.copyOf(initWriteInstructions);
		this.migrationInstructions = ImmutableList.copyOf(migrationInstructions);
		this.readInstructions = ImmutableList.copyOf(readInstructions);
		this.writeInstructions = ImmutableList.copyOf(writeInstructions);
		this.drainInstructions = ImmutableList.copyOf(drainInstructions);

		List<Map<Token, Integer>> capacityRequirements = new ArrayList<>();
		for (ReadInstruction i : Iterables.concat(this.initReadInstructions, this.readInstructions))
			capacityRequirements.add(i.getMinimumBufferCapacity());
		for (WriteInstruction i : Iterables.concat(this.initWriteInstructions, this.writeInstructions))
			capacityRequirements.add(i.getMinimumBufferCapacity());
		this.minimumBufferCapacity = CollectionUtils.union(new Maps.EntryTransformer<Token, List<Integer>, Integer>() {
			@Override
			public Integer transformEntry(Token key, List<Integer> value) {
				return Collections.max(value);
			}
		}, capacityRequirements);

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
		if (!inputTokens.contains(token) && !outputTokens.contains(token))
			throw new IllegalArgumentException(token.toString()+" not an input or output of this blob");
		return minimumBufferCapacity.get(token);
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

		for (ReadInstruction i : Iterables.concat(this.initReadInstructions, this.readInstructions))
			i.init(this.buffers);
		for (WriteInstruction i : Iterables.concat(this.initWriteInstructions, this.writeInstructions))
			i.init(this.buffers);
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
		return drainData;
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
		for (int i = 0; i < initReadInstructions.size(); ++i) {
			ReadInstruction inst = initReadInstructions.get(i);
			while (!inst.load())
				if (isDraining()) {
					doDrain(initReadInstructions.subList(0, i), ImmutableList.<DrainInstruction>of());
					return;
				}
		}

		try {
			initCode.invoke();
		} catch (Throwable ex) {
			barrier.forceTermination();
			throw ex;
		}

		for (WriteInstruction inst : initWriteInstructions)
			inst.run();

		for (Runnable r : migrationInstructions)
			r.run();

		//Show the GC we won't use these anymore.
		initReadInstructions = null;
		initWriteInstructions = null;
		migrationInstructions = null;

		SwitchPoint.invalidateAll(new SwitchPoint[]{sp1});
	}

	private void doAdjust() throws Throwable {
		for (int i = 0; i < readInstructions.size(); ++i) {
			ReadInstruction inst = readInstructions.get(i);
			while (!inst.load())
				if (isDraining()) {
					doDrain(readInstructions.subList(0, i), drainInstructions);
					return;
				}
		}

		for (WriteInstruction inst : writeInstructions) {
			inst.run();
		}

		for (MethodHandle h : storageAdjusts)
			h.invokeExact();
	}

	/**
	 * Extracts elements from storage and puts them in a DrainData for an
	 * interpreter blob.
	 * TODO: extract stateful filter state, probably with an extra
	 * DrainInstruction method or possibly a new instruction class
	 * @param reads read instructions whose load() completed (thus requiring
	 * unload())
	 * @param drains drain instructions, if we're in the steady-state, or an
	 * empty list if we didn't complete init
	 */
	private void doDrain(List<ReadInstruction> reads, List<DrainInstruction> drains) {
		List<Map<Token, Object[]>> data = new ArrayList<>(reads.size() + drains.size());
		for (ReadInstruction i : reads)
			data.add(i.unload());
		for (DrainInstruction i : drains)
			data.add(i.call());
		ImmutableMap<Token, List<Object>> mergedData = CollectionUtils.union(new Maps.EntryTransformer<Token, List<Object[]>, List<Object>>() {
			@Override
			public List<Object> transformEntry(Token key, List<Object[]> value) {
				ImmutableList.Builder<Object> builder = ImmutableList.builder();
				for (Object[] v : value)
					builder.addAll(Arrays.asList(v));
				return builder.build();
			}
		}, data);
		//We have to write our output; the interpreter won't do it for us.
		Predicate<Token> isOutput = Predicates.in(getOutputs());
		for (Map.Entry<Token, List<Object>> e : Maps.filterKeys(mergedData, isOutput).entrySet()) {
			Buffer b = buffers.get(e.getKey());
			Object[] d = e.getValue().toArray();
			for (int written = 0; written < d.length;)
				written += b.write(d, written, d.length-written);
		}
		DrainData forInterp = new DrainData(Maps.filterKeys(mergedData, Predicates.not(isOutput)),
				ImmutableTable.<Integer, String, Object>of());

		Interpreter.InterpreterBlobFactory interpFactory = new Interpreter.InterpreterBlobFactory();
		Blob interp = interpFactory.makeBlob(workers, interpFactory.getDefaultConfiguration(workers), 1, forInterp);
		interp.installBuffers(buffers);
		Runnable interpCode = interp.getCoreCode(0);
		final AtomicBoolean interpFinished = new AtomicBoolean();
		interp.drain(new Runnable() {
			@Override
			public void run() {
				interpFinished.set(true);
			}
		});
		while (!interpFinished.get())
			interpCode.run();
		this.drainData = interp.getDrainData();

		SwitchPoint.invalidateAll(new SwitchPoint[]{sp1, sp2});
		drainCallback.run();
	}

	private boolean isDraining() {
		return drainCallback != null;
	}

	public static interface ReadInstruction {
		public void init(Map<Token, Buffer> buffers);
		public Map<Token, Integer> getMinimumBufferCapacity();
		/**
		 * Loads data items from a Buffer into ConcreteStorage.  Returns true
		 * if the load was successful.  This operation is atomic; either all the
		 * data items are loaded (and load() returns true), or none are and it
		 * returns false.
		 * @return true iff the load succeeded.
		 */
		public boolean load();
		/**
		 * Retrieves data items from a ConcreteStorage.  To be called only after
		 * load() returns true, before executing a steady-state iteration.  This
		 * method only retrieves items loaded by load(); a drain instruction
		 * will retrieve other data.
		 * @return
		 */
		public Map<Token, Object[]> unload();
	}

	public static interface WriteInstruction extends Runnable {
		public void init(Map<Token, Buffer> buffers);
		public Map<Token, Integer> getMinimumBufferCapacity();
	}

	public static interface DrainInstruction extends Callable<Map<Token, Object[]>> {
		@Override
		public Map<Token, Object[]> call();
	}
}
