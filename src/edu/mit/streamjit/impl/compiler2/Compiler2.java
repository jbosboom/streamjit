package edu.mit.streamjit.impl.compiler2;

import com.google.common.base.Function;
import static com.google.common.base.Preconditions.checkState;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeResolver;
import com.google.common.reflect.TypeToken;
import edu.mit.streamjit.api.DuplicateSplitter;
import edu.mit.streamjit.api.IllegalStreamGraphException;
import edu.mit.streamjit.api.Input;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.Output;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.WeightedRoundrobinJoiner;
import edu.mit.streamjit.api.WeightedRoundrobinSplitter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.blob.PeekableBuffer;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.InputBufferFactory;
import edu.mit.streamjit.impl.common.OutputBufferFactory;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.compiler.Schedule;
import edu.mit.streamjit.impl.compiler2.Compiler2BlobHost.DrainInstruction;
import edu.mit.streamjit.impl.compiler2.Compiler2BlobHost.ReadInstruction;
import edu.mit.streamjit.impl.compiler2.Compiler2BlobHost.WriteInstruction;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.apps.fmradio.FMRadio;
import edu.mit.streamjit.util.CollectionUtils;
import edu.mit.streamjit.util.Combinators;
import static edu.mit.streamjit.util.LookupUtils.findStatic;
import edu.mit.streamjit.util.Pair;
import edu.mit.streamjit.util.ReflectionUtils;
import edu.mit.streamjit.util.bytecode.Module;
import edu.mit.streamjit.util.bytecode.ModuleClassLoader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 9/22/2013
 */
public class Compiler2 {
	public static final ImmutableSet<Class<?>> REMOVABLE_WORKERS = ImmutableSet.<Class<?>>of(
			RoundrobinSplitter.class, WeightedRoundrobinSplitter.class, DuplicateSplitter.class,
			RoundrobinJoiner.class, WeightedRoundrobinJoiner.class);
	public static final ImmutableSet<IndexFunctionTransformer> INDEX_FUNCTION_TRANSFORMERS = ImmutableSet.<IndexFunctionTransformer>of(
			new IdentityIndexFunctionTransformer()
//			new ArrayifyIndexFunctionTransformer(false),
//			new ArrayifyIndexFunctionTransformer(true)
	);
	public static final RemovalStrategy REMOVAL_STRATEGY = new BitsetRemovalStrategy();
	public static final FusionStrategy FUSION_STRATEGY = new BitsetFusionStrategy();
	public static final UnboxingStrategy UNBOXING_STRATEGY = new BitsetUnboxingStrategy();
	public static final AllocationStrategy ALLOCATION_STRATEGY = new SubsetBiasAllocationStrategy(8);
	public static final StorageStrategy INTERNAL_STORAGE_STRATEGY = new TuneInternalStorageStrategy();
	public static final StorageStrategy EXTERNAL_STORAGE_STRATEGY = new TuneExternalStorageStrategy();
	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private static final AtomicInteger PACKAGE_NUMBER = new AtomicInteger();
	private final ImmutableSet<Worker<?, ?>> workers;
	private final ImmutableSet<ActorArchetype> archetypes;
	private final NavigableSet<Actor> actors;
	private ImmutableSortedSet<ActorGroup> groups;
	private ImmutableSortedSet<WorkerActor> actorsToBeRemoved;
	private final Configuration config;
	private final int maxNumCores;
	private final DrainData initialState;
	/**
	 * If the blob is the entire graph, this is the overall input; else null.
	 */
	private final Input<?> overallInput;
	/**
	 * If the blob is the entire graph, this is the overall output; else null.
	 */
	private final Output<?> overallOutput;
	private Buffer overallInputBuffer, overallOutputBuffer;
	private ImmutableMap<Token, Buffer> precreatedBuffers;
	private final ImmutableMap<Token, ImmutableList<Object>> initialStateDataMap;
	private final Set<Storage> storage;
	private ImmutableMap<ActorGroup, Integer> externalSchedule;
	private final Module module = new Module();
	private final ModuleClassLoader classloader = new ModuleClassLoader(module);
	private final String packageName = "compiler"+PACKAGE_NUMBER.getAndIncrement();
	private ImmutableMap<ActorGroup, Integer> initSchedule;
	/**
	 * For each token in the blob, the number of items live on that edge after
	 * the init schedule, without regard to removals.  (We could recover this
	 * information from Actor.inputSlots when we're creating drain instructions,
	 * but storing it simplifies the code and permits asserting we didn't lose
	 * any items.)
	 */
	private ImmutableMap<Token, Integer> postInitLiveness;
	/**
	 * ConcreteStorage instances used during initialization (bound into the
	 * initialization code).
	 */
	private ImmutableMap<Storage, ConcreteStorage> initStorage;
	/**
	 * ConcreteStorage instances used during the steady-state (bound into the
	 * steady-state code).
	 */
	private ImmutableMap<Storage, ConcreteStorage> steadyStateStorage;
	/**
	 * Code to run the initialization schedule.  (Initialization is
	 * single-threaded.)
	 */
	private MethodHandle initCode;
	/**
	 * Code to run the steady state schedule.  The blob host takes care of
	 * filling/flushing buffers, adjusting storage and the global barrier.
	 */
	private ImmutableList<MethodHandle> steadyStateCode;
	private final List<ReadInstruction> initReadInstructions = new ArrayList<>();
	private final List<WriteInstruction> initWriteInstructions = new ArrayList<>();
	private final List<Runnable> migrationInstructions = new ArrayList<>();
	private final List<ReadInstruction> readInstructions = new ArrayList<>();
	private final List<WriteInstruction> writeInstructions = new ArrayList<>();
	private final List<DrainInstruction> drainInstructions = new ArrayList<>();
	public Compiler2(Set<Worker<?, ?>> workers, Configuration config, int maxNumCores, DrainData initialState, Input<?> input, Output<?> output) {
		this.workers = ImmutableSet.copyOf(workers);
		Map<Class<?>, ActorArchetype> archetypesBuilder = new HashMap<>();
		Map<Worker<?, ?>, WorkerActor> workerActors = new HashMap<>();
		for (Worker<?, ?> w : workers) {
			@SuppressWarnings("unchecked")
			Class<? extends Worker<?, ?>> wClass = (Class<? extends Worker<?, ?>>)w.getClass();
			if (archetypesBuilder.get(wClass) == null)
				archetypesBuilder.put(wClass, new ActorArchetype(wClass, module));
			WorkerActor actor = new WorkerActor(w, archetypesBuilder.get(wClass));
			workerActors.put(w, actor);
		}
		this.archetypes = ImmutableSet.copyOf(archetypesBuilder.values());

		Map<Token, TokenActor> tokenActors = new HashMap<>();
		Table<Actor, Actor, Storage> storageTable = HashBasedTable.create();
		int[] inputTokenId = new int[]{Integer.MIN_VALUE}, outputTokenId = new int[]{Integer.MAX_VALUE};
		for (WorkerActor a : workerActors.values())
			a.connect(ImmutableMap.copyOf(workerActors), tokenActors, storageTable, inputTokenId, outputTokenId);
		this.actors = new TreeSet<>();
		this.actors.addAll(workerActors.values());
		this.actors.addAll(tokenActors.values());
		this.storage = new HashSet<>(storageTable.values());

		this.config = config;
		this.maxNumCores = maxNumCores;
		this.initialState = initialState;
		ImmutableMap.Builder<Token, ImmutableList<Object>> initialStateDataMapBuilder = ImmutableMap.builder();
		if (initialState != null) {
			for (Table.Cell<Actor, Actor, Storage> cell : storageTable.cellSet()) {
				Token tok;
				if (cell.getRowKey() instanceof TokenActor)
					tok = ((TokenActor)cell.getRowKey()).token();
				else if (cell.getColumnKey() instanceof TokenActor)
					tok = ((TokenActor)cell.getColumnKey()).token();
				else
					tok = new Token(((WorkerActor)cell.getRowKey()).worker(),
							((WorkerActor)cell.getColumnKey()).worker());
				ImmutableList<Object> data = initialState.getData(tok);
				if (data != null && !data.isEmpty()) {
					initialStateDataMapBuilder.put(tok, data);
					cell.getValue().initialData().add(Pair.make(data, MethodHandles.identity(int.class)));
				}
			}
		}
		this.initialStateDataMap = initialStateDataMapBuilder.build();
		this.overallInput = input;
		this.overallOutput = output;
	}

	public Blob compile() {
		findRemovals();
		fuse();
		schedule();

//		identityRemoval();
		splitterRemoval();
		joinerRemoval();

		inferTypes();
		unbox();

		generateArchetypalCode();
		createBuffers();
		createInitCode();
		createSteadyStateCode();
		return instantiateBlob();
	}

	private void findRemovals() {
		ImmutableSortedSet.Builder<WorkerActor> builder = ImmutableSortedSet.naturalOrder();
		next_worker: for (WorkerActor a : Iterables.filter(actors, WorkerActor.class)) {
			if (!REMOVABLE_WORKERS.contains(a.worker().getClass())) continue;
			for (Storage s : a.outputs())
				if (!s.initialData().isEmpty())
					continue next_worker;
			for (Storage s : a.inputs())
				if (!s.initialData().isEmpty())
					continue next_worker;
			if (!REMOVAL_STRATEGY.remove(a, config)) continue;
			builder.add(a);
		}
		this.actorsToBeRemoved = builder.build();
	}

	/**
	 * Fuses actors into groups as directed by the configuration.
	 */
	private void fuse() {
		List<ActorGroup> actorGroups = new ArrayList<>();
		for (Actor a : actors)
			actorGroups.add(ActorGroup.of(a));

		//Fuse as much as possible.
		just_fused: do {
			try_fuse: for (Iterator<ActorGroup> it = actorGroups.iterator(); it.hasNext();) {
				ActorGroup g = it.next();
				if (g.isTokenGroup())
					continue try_fuse;
				for (ActorGroup pg : g.predecessorGroups())
					if (pg.isTokenGroup())
						continue try_fuse;
				if (g.isPeeking() || g.predecessorGroups().size() > 1)
					continue try_fuse;
				for (Storage s : g.inputs())
					if (!s.initialData().isEmpty())
						continue try_fuse;

				//We are assuming FusionStrategies are all happy to work
				//group-by-group.  If later we want to make all decisions at
				//once, we'll refactor existing FusionStrategies to inherit from
				//a base class containing this loop.
				if (!FUSION_STRATEGY.fuseUpward(g, config))
					continue try_fuse;

				ActorGroup gpred = Iterables.getOnlyElement(g.predecessorGroups());
				ActorGroup fusedGroup = ActorGroup.fuse(g, gpred);
				it.remove();
				actorGroups.remove(gpred);
				actorGroups.add(fusedGroup);
				continue just_fused;
			}
			break;
		} while (true);

		this.groups = ImmutableSortedSet.copyOf(actorGroups);

		Boolean reportFusion = (Boolean)config.getExtraData("reportFusion");
		if (reportFusion != null && reportFusion) {
			for (ActorGroup g : groups) {
				if (g.isTokenGroup()) continue;
				List<Integer> list = new ArrayList<>();
				for (Actor a : g.actors())
					list.add(a.id());
				System.out.println(com.google.common.base.Joiner.on(' ').join(list));
			}
			System.out.flush();
			System.exit(0);
		}
	}

	/**
	 * Computes each group's internal schedule and the external schedule.
	 */
	private void schedule() {
		for (ActorGroup g : groups)
			internalSchedule(g);
		externalSchedule();
		initSchedule();
	}

	private void externalSchedule() {
		Schedule.Builder<ActorGroup> scheduleBuilder = Schedule.builder();
		scheduleBuilder.addAll(groups);
		for (ActorGroup g : groups) {
			for (Storage e : g.outputs()) {
				Actor upstream = Iterables.getOnlyElement(e.upstream());
				Actor downstream = Iterables.getOnlyElement(e.downstream());
				ActorGroup other = downstream.group();
				int upstreamAdjust = g.schedule().get(upstream);
				int downstreamAdjust = other.schedule().get(downstream);
				scheduleBuilder.connect(g, other)
						.push(e.push() * upstreamAdjust)
						.pop(e.pop() * downstreamAdjust)
						.peek(e.peek() * downstreamAdjust)
						.bufferExactly(0);
			}
		}
		int multiplier = config.getParameter("multiplier", IntParameter.class).getValue();
		scheduleBuilder.multiply(multiplier);
		try {
			externalSchedule = scheduleBuilder.build().getSchedule();
		} catch (Schedule.ScheduleException ex) {
			throw new StreamCompilationFailedException("couldn't find external schedule; mult = "+multiplier, ex);
		}
	}

	/**
	 * Computes the internal schedule for the given group.
	 */
	private void internalSchedule(ActorGroup g) {
		Schedule.Builder<Actor> scheduleBuilder = Schedule.builder();
		scheduleBuilder.addAll(g.actors());
		for (Actor a : g.actors())
			scheduleBuilder.executeAtLeast(a, 1);
		for (Storage s : g.internalEdges()) {
			scheduleBuilder.connect(Iterables.getOnlyElement(s.upstream()), Iterables.getOnlyElement(s.downstream()))
					.push(s.push())
					.pop(s.pop())
					.peek(s.peek())
					.bufferExactly(0);
		}

		try {
			Schedule<Actor> schedule = scheduleBuilder.build();
			g.setSchedule(schedule.getSchedule());
		} catch (Schedule.ScheduleException ex) {
			throw new StreamCompilationFailedException("couldn't find internal schedule for group "+g+"\n"+scheduleBuilder.toString(), ex);
		}
	}

	private void initSchedule() {
		Schedule.Builder<ActorGroup> scheduleBuilder = Schedule.builder();
		scheduleBuilder.addAll(groups);
		for (Storage s : storage) {
			if (s.isInternal()) continue;
			Actor upstream = Iterables.getOnlyElement(s.upstream()), downstream = Iterables.getOnlyElement(s.downstream());
			int upstreamAdjust = upstream.group().schedule().get(upstream);
			int downstreamAdjust = downstream.group().schedule().get(downstream);
			int throughput, excessPeeks;
			//TODO: avoid double-buffering token groups here?
			if (actorsToBeRemoved.contains(downstream) && false)
				throughput = excessPeeks = 0;
			else {
				throughput = s.push() * upstreamAdjust * externalSchedule.get(upstream.group());
				excessPeeks = Math.max(s.peek() - s.pop(), 0);
			}
			int initialDataSize = Iterables.getOnlyElement(s.initialData(), new Pair<>(ImmutableList.<Object>of(), (MethodHandle)null)).first.size();
			scheduleBuilder.connect(upstream.group(), downstream.group())
					.push(s.push() * upstreamAdjust)
					.pop(s.pop() * downstreamAdjust)
					.peek(s.peek() * downstreamAdjust)
					.bufferAtLeast(throughput + excessPeeks - initialDataSize);
		}

		IntParameter initBufferingCostParam = config.getParameter("InitBufferingCost", IntParameter.class);
		int initBufferCost = initBufferingCostParam.getValue(), fireCost = initBufferingCostParam.getMax() - initBufferCost;
		scheduleBuilder.costs(fireCost, initBufferCost);
		try {
			Schedule<ActorGroup> schedule = scheduleBuilder.build();
			this.initSchedule = schedule.getSchedule();
		} catch (Schedule.ScheduleException ex) {
			throw new StreamCompilationFailedException("couldn't find init schedule", ex);
		}

		ImmutableMap.Builder<Token, Integer> postInitLivenessBuilder = ImmutableMap.builder();
		for (Storage s : storage) {
			if (s.isInternal()) continue;
			Actor upstream = Iterables.getOnlyElement(s.upstream()), downstream = Iterables.getOnlyElement(s.downstream());
			int upstreamExecutions = upstream.group().schedule().get(upstream) * initSchedule.get(upstream.group());
			int downstreamExecutions = downstream.group().schedule().get(downstream) * initSchedule.get(downstream.group());
			int liveItems = s.push() * upstreamExecutions - s.pop() * downstreamExecutions + s.initialDataIndices().size();
			assert liveItems >= 0 : s;

			int index = downstream.inputs().indexOf(s);
			assert index != -1;
			Token token;
			if (downstream instanceof WorkerActor) {
				Worker<?, ?> w = ((WorkerActor)downstream).worker();
				token = downstream.id() == 0 ? Token.createOverallInputToken(w) :
						new Token(Workers.getPredecessors(w).get(index), w);
			} else
				token = ((TokenActor)downstream).token();
			for (int i = 0; i < liveItems; ++i)
				downstream.inputSlots(index).add(StorageSlot.live(token, i));
			postInitLivenessBuilder.put(token, liveItems);
		}
		this.postInitLiveness = postInitLivenessBuilder.build();

		int initScheduleSize = 0;
		for (int i : initSchedule.values())
			initScheduleSize += i;
//		System.out.println("init schedule size "+initScheduleSize);
		int totalBuffering = 0;
		for (int i : postInitLiveness.values())
			totalBuffering += i;
//		System.out.println("total items buffered "+totalBuffering);
	}

	private void splitterRemoval() {
		for (WorkerActor splitter : actorsToBeRemoved) {
			if (!(splitter.worker() instanceof Splitter)) continue;
			List<MethodHandle> transfers = splitterTransferFunctions(splitter);
			Storage survivor = Iterables.getOnlyElement(splitter.inputs());
			//Remove all instances of splitter, not just the first.
			survivor.downstream().removeAll(ImmutableList.of(splitter));
			MethodHandle Sin = Iterables.getOnlyElement(splitter.inputIndexFunctions());
			List<StorageSlot> drainInfo = splitter.inputSlots(0);
			for (int i = 0; i < splitter.outputs().size(); ++i) {
				Storage victim = splitter.outputs().get(i);
				MethodHandle t = transfers.get(i);
				for (Actor a : victim.downstream()) {
					List<Storage> inputs = a.inputs();
					List<MethodHandle> inputIndices = a.inputIndexFunctions();
					for (int j = 0; j < inputs.size(); ++j)
						if (inputs.get(j).equals(victim)) {
							inputs.set(j, survivor);
							survivor.downstream().add(a);
							inputIndices.set(j, MethodHandles.filterReturnValue(inputIndices.get(j), t));
							if (splitter.push(i) > 0)
								for (int idx = 0, q = a.translateInputIndex(j, idx); q < drainInfo.size(); ++idx, q = a.translateInputIndex(j, idx)) {
									a.inputSlots(j).add(drainInfo.get(q));
									drainInfo.set(q, drainInfo.get(q).duplify());
								}
							inputIndices.set(j, MethodHandles.filterReturnValue(inputIndices.get(j), Sin));
						}
				}

				for (Pair<ImmutableList<Object>, MethodHandle> item : victim.initialData())
					survivor.initialData().add(new Pair<>(item.first, MethodHandles.filterReturnValue(item.second, t)));
				storage.remove(victim);
			}

			removeActor(splitter);
			assert consistency();
		}
	}

	/**
	 * Returns transfer functions for the given splitter.
	 *
	 * A splitter has one transfer function for each output that maps logical
	 * output indices to logical input indices (representing the splitter's
	 * distribution pattern).
	 * @param a an actor
	 * @return transfer functions, or null
	 */
	private List<MethodHandle> splitterTransferFunctions(WorkerActor a) {
		assert REMOVABLE_WORKERS.contains(a.worker().getClass()) : a.worker().getClass();
		if (a.worker() instanceof RoundrobinSplitter || a.worker() instanceof WeightedRoundrobinSplitter) {
			int[] weights = new int[a.outputs().size()];
			for (int i = 0; i < weights.length; ++i)
				weights[i] = a.push(i);
			return roundrobinTransferFunctions(weights);
		} else if (a.worker() instanceof DuplicateSplitter) {
			return Collections.nCopies(a.outputs().size(), MethodHandles.identity(int.class));
		} else
			throw new AssertionError();
	}

	private void joinerRemoval() {
		for (WorkerActor joiner : actorsToBeRemoved) {
			if (!(joiner.worker() instanceof Joiner)) continue;
			List<MethodHandle> transfers = joinerTransferFunctions(joiner);
			Storage survivor = Iterables.getOnlyElement(joiner.outputs());
			//Remove all instances of joiner, not just the first.
			survivor.upstream().removeAll(ImmutableList.of(joiner));
			MethodHandle Jout = Iterables.getOnlyElement(joiner.outputIndexFunctions());
			for (int i = 0; i < joiner.inputs().size(); ++i) {
				Storage victim = joiner.inputs().get(i);
				MethodHandle t = transfers.get(i);
				MethodHandle t2 = MethodHandles.filterReturnValue(t, Jout);
				for (Actor a : victim.upstream()) {
					List<Storage> outputs = a.outputs();
					List<MethodHandle> outputIndices = a.outputIndexFunctions();
					for (int j = 0; j < outputs.size(); ++j)
						if (outputs.get(j).equals(victim)) {
							outputs.set(j, survivor);
							outputIndices.set(j, MethodHandles.filterReturnValue(outputIndices.get(j), t2));
							survivor.upstream().add(a);
						}
				}

				for (Pair<ImmutableList<Object>, MethodHandle> item : victim.initialData())
					survivor.initialData().add(new Pair<>(item.first, MethodHandles.filterReturnValue(item.second, t2)));
				storage.remove(victim);
			}

			//Linearize drain info from the joiner's inputs.
			int maxIdx = 0;
			for (int i = 0; i < joiner.inputs().size(); ++i) {
				MethodHandle t = transfers.get(i);
				for (int idx = 0; idx < joiner.inputSlots(i).size(); ++idx)
					try {
						maxIdx = Math.max(maxIdx, (int)t.invokeExact(joiner.inputSlots(i).size()-1));
					} catch (Throwable ex) {
						throw new AssertionError("Can't happen! transfer function threw?", ex);
					}
			}
			List<StorageSlot> linearizedInput = new ArrayList<>(Collections.nCopies(maxIdx+1, StorageSlot.hole()));
			for (int i = 0; i < joiner.inputs().size(); ++i) {
				MethodHandle t = transfers.get(i);
				for (int idx = 0; idx < joiner.inputSlots(i).size(); ++idx)
					try {
						linearizedInput.set((int)t.invokeExact(idx), joiner.inputSlots(i).get(idx));
					} catch (Throwable ex) {
						throw new AssertionError("Can't happen! transfer function threw?", ex);
					}
				joiner.inputSlots(i).clear();
				joiner.inputSlots(i).trimToSize();
			}

			if (!linearizedInput.isEmpty()) {
				for (Actor a : survivor.downstream())
					for (int j = 0; j < a.inputs().size(); ++j)
						if (a.inputs().get(j).equals(survivor))
							for (int idx = 0, q = a.translateInputIndex(j, idx); q < linearizedInput.size(); ++idx, q = a.translateInputIndex(j, idx)) {
								StorageSlot slot = linearizedInput.get(q);
								a.inputSlots(j).add(slot);
								linearizedInput.set(q, slot.duplify());
							}
			}

//			System.out.println("removed "+joiner);
			removeActor(joiner);
			assert consistency();
		}
	}

	private List<MethodHandle> joinerTransferFunctions(WorkerActor a) {
		assert REMOVABLE_WORKERS.contains(a.worker().getClass()) : a.worker().getClass();
		if (a.worker() instanceof RoundrobinJoiner || a.worker() instanceof WeightedRoundrobinJoiner) {
			int[] weights = new int[a.inputs().size()];
			for (int i = 0; i < weights.length; ++i)
				weights[i] = a.pop(i);
			return roundrobinTransferFunctions(weights);
		} else
			throw new AssertionError();
	}

	private List<MethodHandle> roundrobinTransferFunctions(int[] weights) {
		int[] weightPrefixSum = new int[weights.length + 1];
		for (int i = 1; i < weightPrefixSum.length; ++i)
			weightPrefixSum[i] = weightPrefixSum[i-1] + weights[i-1];
		int N = weightPrefixSum[weightPrefixSum.length-1];
		//t_x(i) = N(i/w[x]) + sum_0_x-1{w} + (i mod w[x])
		//where the first two terms select a "window" and the third is the
		//index into that window.
		ImmutableList.Builder<MethodHandle> transfer = ImmutableList.builder();
		for (int x = 0; x < weights.length; ++x)
			transfer.add(MethodHandles.insertArguments(ROUNDROBIN_TRANSFER_FUNCTION, 0, weights[x], weightPrefixSum[x], N));
		return transfer.build();
	}
	private final MethodHandle ROUNDROBIN_TRANSFER_FUNCTION = findStatic(LOOKUP, Compiler2.class, "_roundrobinTransferFunction", int.class, int.class, int.class, int.class, int.class);
	//TODO: build this directly out of MethodHandles?
	private static int _roundrobinTransferFunction(int weight, int prefixSum, int N, int i) {
		//assumes nonnegative indices
		return N*(i/weight) + prefixSum + (i % weight);
	}

	/**
	 * Removes an Actor from this compiler's data structures.  The Actor should
	 * already have been unlinked from the graph (no incoming edges); this takes
	 * care of removing it from the actors set, its actor group (possibly
	 * removing the group if it's now empty), and the schedule.
	 * @param a the actor to remove
	 */
	private void removeActor(Actor a) {
		assert actors.contains(a) : a;
		actors.remove(a);
		ActorGroup g = a.group();
		g.remove(a);
		if (g.actors().isEmpty()) {
			groups = ImmutableSortedSet.copyOf(Sets.difference(groups, ImmutableSet.of(g)));
			externalSchedule = ImmutableMap.copyOf(Maps.difference(externalSchedule, ImmutableMap.of(g, 0)).entriesOnlyOnLeft());
			initSchedule = ImmutableMap.copyOf(Maps.difference(initSchedule, ImmutableMap.of(g, 0)).entriesOnlyOnLeft());
		}
	}

	private boolean consistency() {
		Set<Storage> usedStorage = new HashSet<>();
		for (Actor a : actors) {
			usedStorage.addAll(a.inputs());
			usedStorage.addAll(a.outputs());
		}
		if (!storage.equals(usedStorage)) {
			Set<Storage> unused = Sets.difference(storage, usedStorage);
			Set<Storage> untracked = Sets.difference(usedStorage, storage);
			throw new AssertionError(String.format("inconsistent storage:%n\tunused: %s%n\tuntracked:%s%n", unused, untracked));
		}
		return true;
	}

	//<editor-fold defaultstate="collapsed" desc="Unimplemented optimization stuff">
//	/**
//	 * Removes Identity instances from the graph, unless doing so would make the
//	 * graph empty.
//	 */
//	private void identityRemoval() {
//		//TODO: remove from group, possibly removing the group if it becomes empty
//		for (Iterator<Actor> iter = actors.iterator(); iter.hasNext();) {
//			if (actors.size() == 1)
//				break;
//			Actor actor = iter.next();
//			if (!actor.archetype().workerClass().equals(Identity.class))
//				continue;
//
//			iter.remove();
//			assert actor.predecessors().size() == 1 && actor.successors().size() == 1;
//			Object upstream = actor.predecessors().get(0), downstream = actor.successors().get(0);
//			if (upstream instanceof Actor)
//				replace(((Actor)upstream).successors(), actor, downstream);
//			if (downstream instanceof Actor)
//				replace(((Actor)downstream).predecessors(), actor, upstream);
//			//No index function changes required for Identity actors.
//		}
//	}
//
//	private static int replace(List<Object> list, Object target, Object replacement) {
//		int replacements = 0;
//		for (int i = 0; i < list.size(); ++i)
//			if (Objects.equals(list.get(0), target)) {
//				list.set(i, replacement);
//				++replacements;
//			}
//		return replacements;
//	}
	//</editor-fold>

	/**
	 * Performs type inference to replace type variables with concrete types.
	 * For now, we only care about wrapper types.
	 */
	public void inferTypes() {
		while (inferUpward() || inferDownward());
	}

	private boolean inferUpward() {
		boolean changed = false;
		//For each storage, if a reader's input type is a final type, all
		//writers' output types must be that final type.  (Wrappers are final,
		//so this works for wrappers, and maybe detects errors related to other
		//types.)
		for (Storage s : storage) {
			Set<TypeToken<?>> finalInputTypes = new HashSet<>();
			for (Actor a : s.downstream())
				if (Modifier.isFinal(a.inputType().getRawType().getModifiers()))
					finalInputTypes.add(a.inputType());
			if (finalInputTypes.isEmpty()) continue;
			if (finalInputTypes.size() > 1)
				throw new IllegalStreamGraphException("Type mismatch among readers: "+s.downstream());

			TypeToken<?> inputType = finalInputTypes.iterator().next();
			for (Actor a : s.upstream())
				if (!a.outputType().equals(inputType)) {
					TypeToken<?> oldOutputType = a.outputType();
					TypeResolver resolver = new TypeResolver().where(oldOutputType.getType(), inputType.getType());
					TypeToken<?> newOutputType = TypeToken.of(resolver.resolveType(oldOutputType.getType()));
					if (!oldOutputType.equals(newOutputType)) {
						a.setOutputType(newOutputType);
//						System.out.println("inferUpward: inferred "+a+" output type: "+oldOutputType+" -> "+newOutputType);
						changed = true;
					}

					TypeToken<?> oldInputType = a.inputType();
					TypeToken<?> newInputType = TypeToken.of(resolver.resolveType(oldInputType.getType()));
					if (!oldInputType.equals(newInputType)) {
						a.setInputType(newInputType);
//						System.out.println("inferUpward: inferred "+a+" input type: "+oldInputType+" -> "+newInputType);
						changed = true;
					}
				}
		}
		return changed;
	}

	private boolean inferDownward() {
		boolean changed = false;
		//For each storage, find the most specific common type among all the
		//writers' output types, then if it's final, unify with any variable or
		//wildcard reader input type.  (We only unify if final to avoid removing
		//a type variable too soon.  We could also refine concrete types like
		//Object to a more specific subclass.)
		for (Storage s : storage) {
			Set<? extends TypeToken<?>> commonTypes = null;
			for (Actor a : s.upstream())
				if (commonTypes == null)
					commonTypes = a.outputType().getTypes();
				else
					commonTypes = Sets.intersection(commonTypes, a.outputType().getTypes());
			if (commonTypes.isEmpty())
				throw new IllegalStreamGraphException("No common type among writers: "+s.upstream());

			TypeToken<?> mostSpecificType = commonTypes.iterator().next();
			if (!Modifier.isFinal(mostSpecificType.getRawType().getModifiers()))
				continue;
			for (Actor a : s.downstream()) {
				TypeToken<?> oldInputType = a.inputType();
				//TODO: this isn't quite right?
				if (!ReflectionUtils.containsVariableOrWildcard(oldInputType.getType())) continue;

				TypeResolver resolver = new TypeResolver().where(oldInputType.getType(), mostSpecificType.getType());
				TypeToken<?> newInputType = TypeToken.of(resolver.resolveType(oldInputType.getType()));
				if (!oldInputType.equals(newInputType)) {
					a.setInputType(newInputType);
//					System.out.println("inferDownward: inferred "+a+" input type: "+oldInputType+" -> "+newInputType);
					changed = true;
				}

				TypeToken<?> oldOutputType = a.outputType();
				TypeToken<?> newOutputType = TypeToken.of(resolver.resolveType(oldOutputType.getType()));
				if (!oldOutputType.equals(newOutputType)) {
					a.setOutputType(newOutputType);
//					System.out.println("inferDownward: inferred "+a+" output type: "+oldOutputType+" -> "+newOutputType);
					changed = true;
				}
			}
		}
		return changed;
	}

	/**
	 * Unboxes storage types and Actor input and output types.
	 */
	private void unbox() {
		for (Storage s : storage) {
			if (isUnboxable(s.contentType()) && UNBOXING_STRATEGY.unboxStorage(s, config)) {
				TypeToken<?> contents = s.contentType();
				Class<?> type = contents.unwrap().getRawType();
				s.setType(type);
//				if (!s.type().equals(contents.getRawType()))
//					System.out.println("unboxed "+s+" to "+type);
			}
		}

		for (WorkerActor a : Iterables.filter(actors, WorkerActor.class)) {
			if (isUnboxable(a.inputType()) && UNBOXING_STRATEGY.unboxInput(a, config)) {
				TypeToken<?> oldType = a.inputType();
				a.setInputType(oldType.unwrap());
//				if (!a.inputType().equals(oldType))
//					System.out.println("unboxed input of "+a+": "+oldType+" -> "+a.inputType());
			}
			if (isUnboxable(a.outputType()) && UNBOXING_STRATEGY.unboxOutput(a, config)) {
				TypeToken<?> oldType = a.outputType();
				a.setOutputType(oldType.unwrap());
//				if (!a.outputType().equals(oldType))
//					System.out.println("unboxed output of "+a+": "+oldType+" -> "+a.outputType());
			}
		}
	}

	private boolean isUnboxable(TypeToken<?> type) {
		return Primitives.isWrapperType(type.getRawType()) && !type.getRawType().equals(Void.class);
	}

	private void generateArchetypalCode() {
		for (final ActorArchetype archetype : archetypes) {
			Iterable<WorkerActor> workerActors = FluentIterable.from(actors)
					.filter(WorkerActor.class)
					.filter(new Predicate<WorkerActor>() {
						@Override
						public boolean apply(WorkerActor input) {
							return input.archetype().equals(archetype);
						}
					});
			archetype.generateCode(packageName, classloader, workerActors);
			for (WorkerActor wa : workerActors)
				wa.setStateHolder(archetype.makeStateHolder(wa));
		}
	}

	/**
	 * If we're compiling an entire graph, create the overall input and output
	 * buffers now so we can take advantage of
	 * PeekableBuffers/PokeableBuffers.  Otherwise we must pre-init()
	 * our read/write instructions to refer to these buffers, since they won't
	 * be passed to the blob's installBuffers().
	 */
	private void createBuffers() {
		assert (overallInput == null) == (overallOutput == null);
		if (overallInput == null) {
			this.precreatedBuffers = ImmutableMap.of();
			return;
		}

		ActorGroup inputGroup = null, outputGroup = null;
		Token inputToken = null, outputToken = null;
		for (ActorGroup g : groups)
			if (g.isTokenGroup()) {
				assert g.actors().size() == 1;
				TokenActor ta = (TokenActor)g.actors().iterator().next();
				assert g.schedule().get(ta) == 1;
				if (ta.isInput()) {
					assert inputGroup == null;
					inputGroup = g;
					inputToken = ta.token();
				}
				if (ta.isOutput()) {
					assert outputGroup == null;
					outputGroup = g;
					outputToken = ta.token();
				}
			}
		this.overallInputBuffer = InputBufferFactory.unwrap(overallInput).createReadableBuffer(
				Math.max(initSchedule.get(inputGroup), externalSchedule.get(inputGroup)));
		this.overallOutputBuffer = OutputBufferFactory.unwrap(overallOutput).createWritableBuffer(
				Math.max(initSchedule.get(outputGroup), externalSchedule.get(outputGroup)));
		this.precreatedBuffers = ImmutableMap.<Token, Buffer>builder()
				.put(inputToken, overallInputBuffer)
				.put(outputToken, overallOutputBuffer)
				.build();
	}

	private void createInitCode() {
		ImmutableMap<Actor, ImmutableList<MethodHandle>> indexFxnBackup = adjustOutputIndexFunctions(new Function<Storage, Set<Integer>>() {
			@Override
			public Set<Integer> apply(Storage input) {
				return input.initialDataIndices();
			}
		});

		this.initStorage = createStorage(false, new PeekPokeStorageFactory(InternalArrayConcreteStorage.initFactory(initSchedule)));
		initReadInstructions.add(new InitDataReadInstruction(initStorage, initialStateDataMap));

		ImmutableMap<Storage, ConcreteStorage> internalStorage = createStorage(true, InternalArrayConcreteStorage.initFactory(initSchedule));
		IndexFunctionTransformer ift = new IdentityIndexFunctionTransformer();
		ImmutableTable.Builder<Actor, Integer, IndexFunctionTransformer> inputTransformers = ImmutableTable.builder(),
				outputTransformers = ImmutableTable.builder();
		for (Actor a : Iterables.filter(actors, WorkerActor.class)) {
			for (int i = 0; i < a.inputs().size(); ++i)
				inputTransformers.put(a, i, ift);
			for (int i = 0; i < a.outputs().size(); ++i)
				outputTransformers.put(a, i, ift);
		}
		ImmutableMap.Builder<ActorGroup, Integer> unrollFactors = ImmutableMap.builder();
		for (ActorGroup g : groups)
			unrollFactors.put(g, 1);

		/**
		 * During init, all (nontoken) groups are assigned to the same Core in
		 * topological order (via the ordering on ActorGroups).  At the same
		 * time we build the token init schedule information required by the
		 * blob host.
		 */
		Core initCore = new Core(CollectionUtils.union(initStorage, internalStorage), unrollFactors.build(), inputTransformers.build(), outputTransformers.build(), new Bytecodifier.Function(module, classloader, packageName+".init"));
		for (ActorGroup g : groups)
			if (!g.isTokenGroup())
				initCore.allocate(g, Range.closedOpen(0, initSchedule.get(g)));
			else {
				assert g.actors().size() == 1;
				TokenActor ta = (TokenActor)g.actors().iterator().next();
				assert g.schedule().get(ta) == 1;
				ConcreteStorage storage = initStorage.get(Iterables.getOnlyElement(ta.isInput() ? g.outputs() : g.inputs()));
				int executions = initSchedule.get(g);
				if (ta.isInput())
					initReadInstructions.add(makeReadInstruction(ta, storage, executions));
				else
					initWriteInstructions.add(makeWriteInstruction(ta, storage, executions));
			}
		this.initCode = initCore.code();

		restoreOutputIndexFunctions(indexFxnBackup);
	}

	private void createSteadyStateCode() {
		for (Actor a : actors) {
			for (int i = 0; i < a.outputs().size(); ++i) {
				Storage s = a.outputs().get(i);
				if (s.isInternal()) continue;
				int itemsWritten = a.push(i) * initSchedule.get(a.group()) * a.group().schedule().get(a);
				a.outputIndexFunctions().set(i, MethodHandles.filterArguments(
						a.outputIndexFunctions().get(i), 0, Combinators.add(MethodHandles.identity(int.class), itemsWritten)));
			}
			for (int i = 0; i < a.inputs().size(); ++i) {
				Storage s = a.inputs().get(i);
				if (s.isInternal()) continue;
				int itemsRead = a.pop(i) * initSchedule.get(a.group()) * a.group().schedule().get(a);
				a.inputIndexFunctions().set(i, MethodHandles.filterArguments(
						a.inputIndexFunctions().get(i), 0, Combinators.add(MethodHandles.identity(int.class), itemsRead)));
			}
		}

		for (Storage s : storage)
			s.computeSteadyStateRequirements(externalSchedule);
		this.steadyStateStorage = createStorage(false, new PeekPokeStorageFactory(EXTERNAL_STORAGE_STRATEGY.asFactory(config)));
		ImmutableMap<Storage, ConcreteStorage> internalStorage = createStorage(true, INTERNAL_STORAGE_STRATEGY.asFactory(config));

		List<Core> ssCores = new ArrayList<>(maxNumCores);
		IndexFunctionTransformer ift = new IdentityIndexFunctionTransformer();
		for (int i = 0; i < maxNumCores; ++i) {
			ImmutableTable.Builder<Actor, Integer, IndexFunctionTransformer> inputTransformers = ImmutableTable.builder(),
					outputTransformers = ImmutableTable.builder();
			for (Actor a : Iterables.filter(actors, WorkerActor.class)) {
				for (int j = 0; j < a.inputs().size(); ++j) {
//					String name = String.format("Core%dWorker%dInput%dIndexFxnTransformer", i, a.id(), j);
//					SwitchParameter<IndexFunctionTransformer> param = config.getParameter(name, SwitchParameter.class, IndexFunctionTransformer.class);
					inputTransformers.put(a, j, ift);
				}
				for (int j = 0; j < a.outputs().size(); ++j) {
//					String name = String.format("Core%dWorker%dOutput%dIndexFxnTransformer", i, a.id(), j);
//					SwitchParameter<IndexFunctionTransformer> param = config.getParameter(name, SwitchParameter.class, IndexFunctionTransformer.class);
					outputTransformers.put(a, j, ift);
				}
			}

			ImmutableMap.Builder<ActorGroup, Integer> unrollFactors = ImmutableMap.builder();
			for (ActorGroup g : groups) {
				if (g.isTokenGroup()) continue;
				IntParameter param = config.getParameter(String.format("UnrollCore%dGroup%d", i, g.id()), IntParameter.class);
				unrollFactors.put(g, param.getValue());
			}

			ssCores.add(new Core(CollectionUtils.union(steadyStateStorage, internalStorage), unrollFactors.build(), inputTransformers.build(), outputTransformers.build(), new Bytecodifier.Function(module, classloader, packageName+".steadystate.")));
		}

		int throughputPerSteadyState = 0;
		for (ActorGroup g : groups)
			if (!g.isTokenGroup())
				ALLOCATION_STRATEGY.allocateGroup(g, Range.closedOpen(0, externalSchedule.get(g)), ssCores, config);
			else {
				assert g.actors().size() == 1;
				TokenActor ta = (TokenActor)g.actors().iterator().next();
				assert g.schedule().get(ta) == 1;
				ConcreteStorage storage = steadyStateStorage.get(Iterables.getOnlyElement(ta.isInput() ? g.outputs() : g.inputs()));
				int executions = externalSchedule.get(g);
				if (ta.isInput())
					readInstructions.add(makeReadInstruction(ta, storage, executions));
				else {
					writeInstructions.add(makeWriteInstruction(ta, storage, executions));
					throughputPerSteadyState += executions;
				}
			}
		ImmutableList.Builder<MethodHandle> steadyStateCodeBuilder = ImmutableList.builder();
		for (Core c : ssCores)
			if (!c.isEmpty())
				steadyStateCodeBuilder.add(c.code());
		//Provide at least one core of code, even if it doesn't do anything; the
		//blob host will still copy inputs to outputs.
		if (steadyStateCodeBuilder.build().isEmpty())
			steadyStateCodeBuilder.add(Combinators.nop());
		this.steadyStateCode = steadyStateCodeBuilder.build();

		createMigrationInstructions();
		createDrainInstructions();

		Boolean reportThroughput = (Boolean)config.getExtraData("reportThroughput");
		if (reportThroughput != null && reportThroughput) {
			ReportThroughputInstruction rti = new ReportThroughputInstruction(throughputPerSteadyState);
			readInstructions.add(rti);
			writeInstructions.add(rti);
		}
	}

	private ReadInstruction makeReadInstruction(TokenActor a, ConcreteStorage cs, int count) {
		assert a.isInput();
		Storage s = Iterables.getOnlyElement(a.outputs());
		MethodHandle idxFxn = Iterables.getOnlyElement(a.outputIndexFunctions());
		ReadInstruction retval;
		if (count == 0)
			retval = new NopReadInstruction(a.token());
		else if (cs instanceof PeekableBufferConcreteStorage)
			retval = new PeekReadInstruction(a, count);
		else if (!s.type().isPrimitive() &&
				cs instanceof BulkWritableConcreteStorage &&
				contiguouslyIncreasing(idxFxn, 0, count)) {
			retval = new BulkReadInstruction(a, (BulkWritableConcreteStorage)cs, count);
		} else
			retval = new TokenReadInstruction(a, cs, count);
//		System.out.println("Made a "+retval+" for "+a.token());
		retval.init(precreatedBuffers);
		return retval;
	}

	private WriteInstruction makeWriteInstruction(TokenActor a, ConcreteStorage cs, int count) {
		assert a.isOutput();
		Storage s = Iterables.getOnlyElement(a.inputs());
		MethodHandle idxFxn = Iterables.getOnlyElement(a.inputIndexFunctions());
		WriteInstruction retval;
		if (count == 0)
			retval = new NopWriteInstruction(a.token());
		else if (!s.type().isPrimitive() &&
				cs instanceof BulkReadableConcreteStorage &&
				contiguouslyIncreasing(idxFxn, 0, count)) {
			retval = new BulkWriteInstruction(a, (BulkReadableConcreteStorage)cs, count);
		} else
			retval = new TokenWriteInstruction(a, cs, count);
//		System.out.println("Made a "+retval+" for "+a.token());
		retval.init(precreatedBuffers);
		return retval;
	}

	private boolean contiguouslyIncreasing(MethodHandle idxFxn, int start, int count) {
		try {
			int prev = (int)idxFxn.invokeExact(start);
			for (int i = start+1; i < count; ++i) {
				int next = (int)idxFxn.invokeExact(i);
				if (next != (prev + 1))
					return false;
				prev = next;
			}
			return true;
		} catch (Throwable ex) {
			throw new AssertionError("index functions should not throw", ex);
		}
	}

	/**
	 * Create migration instructions: Runnables that move live items from
	 * initialization to steady-state storage.
	 */
	private void createMigrationInstructions() {
		for (Storage s : initStorage.keySet()) {
			ConcreteStorage init = initStorage.get(s), steady = steadyStateStorage.get(s);
			if (steady instanceof PeekableBufferConcreteStorage)
				migrationInstructions.add(new PeekMigrationInstruction(
						s, (PeekableBufferConcreteStorage)steady));
			else
				migrationInstructions.add(new MigrationInstruction(s, init, steady));
		}
	}

	/**
	 * Create drain instructions, which collect live items from steady-state
	 * storage when draining.
	 */
	private void createDrainInstructions() {
		Map<Token, List<Pair<ConcreteStorage, Integer>>> drainReads = new HashMap<>();
		for (Map.Entry<Token, Integer> e : postInitLiveness.entrySet())
			drainReads.put(e.getKey(), new ArrayList<>(Collections.nCopies(e.getValue(), (Pair<ConcreteStorage, Integer>)null)));

		for (Actor a : actors) {
			for (int input = 0; input < a.inputs().size(); ++input) {
				ConcreteStorage storage = steadyStateStorage.get(a.inputs().get(input));
				for (int index = 0; index < a.inputSlots(input).size(); ++index) {
					StorageSlot info = a.inputSlots(input).get(index);
					if (info.isDrainable()) {
						Pair<ConcreteStorage, Integer> old = drainReads.get(info.token()).
								set(info.index(), new Pair<>(storage, a.translateInputIndex(input, index)));
						assert old == null : "overwriting "+info;
					}
				}
			}
		}

		for (Map.Entry<Token, List<Pair<ConcreteStorage, Integer>>> e : drainReads.entrySet()) {
			assert !e.getValue().contains(null) : "lost an element from "+e.getKey()+": "+e.getValue();
			for (Iterator<Pair<ConcreteStorage, Integer>> i = e.getValue().iterator(); i.hasNext();)
				if (i.next().first instanceof PeekableBufferConcreteStorage)
					i.remove();
			drainInstructions.add(new XDrainInstruction(e.getKey(), e.getValue()));
		}

		for (WorkerActor wa : Iterables.filter(actors, WorkerActor.class))
			drainInstructions.add(wa.stateHolder());
	}

	//<editor-fold defaultstate="collapsed" desc="Output index function adjust/restore">
	/**
	 * Adjust output index functions to avoid overwriting items in external
	 * storage.  For any actor writing to external storage, we find the
	 * first item that doesn't hit the live index set and add that many
	 * (making that logical item 0 for writers).
	 * @param liveIndexExtractor a function that computes the relevant live
	 * index set for the given external storage
	 * @return the old output index functions, to be restored later
	 */
	private ImmutableMap<Actor, ImmutableList<MethodHandle>> adjustOutputIndexFunctions(Function<Storage, Set<Integer>> liveIndexExtractor) {
		ImmutableMap.Builder<Actor, ImmutableList<MethodHandle>> backup = ImmutableMap.builder();
		for (Actor a : actors) {
			backup.put(a, ImmutableList.copyOf(a.outputIndexFunctions()));
			for (int i = 0; i < a.outputs().size(); ++i) {
				if (a.push(i) == 0) continue; //No writes -- nothing to adjust.
				Storage s = a.outputs().get(i);
				if (s.isInternal())
					continue;
				Set<Integer> liveIndices = liveIndexExtractor.apply(s);
				assert liveIndices != null : s +" "+liveIndexExtractor;
				int offset = 0;
				while (liveIndices.contains(a.translateOutputIndex(i, offset)))
					++offset;
				//Check future indices are also open (e.g., that we aren't
				//alternating hole/not-hole).
				for (int check = 0; check < 100; ++check)
					assert !liveIndices.contains(a.translateOutputIndex(i, offset + check)) : check;
				a.outputIndexFunctions().set(i, Combinators.add(a.outputIndexFunctions().get(i), offset));
			}
		}
		return backup.build();
	}

	/**
	 * Restores output index functions from a backup returned from
	 * {@link #adjustOutputIndexFunctions(com.google.common.base.Function)}.
	 * @param backup the backup to restore
	 */
	private void restoreOutputIndexFunctions(ImmutableMap<Actor, ImmutableList<MethodHandle>> backup) {
		for (Actor a : actors) {
			ImmutableList<MethodHandle> oldFxns = backup.get(a);
			assert oldFxns != null : "no backup for "+a;
			assert oldFxns.size() == a.outputIndexFunctions().size() : "backup for "+a+" is wrong size";
			Collections.copy(a.outputIndexFunctions(), oldFxns);
		}
	}
	//</editor-fold>

	private ImmutableMap<Storage, ConcreteStorage> createStorage(boolean internal, StorageFactory factory) {
		ImmutableMap.Builder<Storage, ConcreteStorage> builder = ImmutableMap.builder();
		for (Storage s : storage)
			if (s.isInternal() == internal)
				builder.put(s, factory.make(s));
		return builder.build();
	}

	/**
	 * Creates special ConcreteStorage implementations for PeekableBuffer and
	 * PokeableBuffer, or falls back to the given factory.
	 */
	private final class PeekPokeStorageFactory implements StorageFactory {
		private final StorageFactory fallback;
		private final boolean usePeekableBuffer;
		private PeekPokeStorageFactory(StorageFactory fallback) {
			this.fallback = fallback;
			SwitchParameter<Boolean> usePeekableBufferParam = config.getParameter("UsePeekableBuffer", SwitchParameter.class, Boolean.class);
			this.usePeekableBuffer = usePeekableBufferParam.getValue();
		}
		@Override
		public ConcreteStorage make(Storage storage) {
			if (storage.id().isOverallInput() && overallInputBuffer instanceof PeekableBuffer && usePeekableBuffer)
				return PeekableBufferConcreteStorage.factory(ImmutableMap.of(storage.id(), (PeekableBuffer)overallInputBuffer)).make(storage);
			//TODO: PokeableBuffer
			return fallback.make(storage);
		}
	}

	private static final class MigrationInstruction implements Runnable {
		private final ConcreteStorage init, steady;
		private final int[] indicesToMigrate;
		private MigrationInstruction(Storage storage, ConcreteStorage init, ConcreteStorage steady) {
			this.init = init;
			this.steady = steady;
			ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
			for (Actor a : storage.downstream())
				for (int i = 0; i < a.inputs().size(); ++i)
					if (a.inputs().get(i).equals(storage))
						for (int idx = 0; idx < a.inputSlots(i).size(); ++idx)
							if (a.inputSlots(i).get(idx).isLive())
								builder.add(a.translateInputIndex(i, idx));
			this.indicesToMigrate = Ints.toArray(builder.build());
		}
		@Override
		public void run() {
			init.sync();
			for (int i : indicesToMigrate)
				steady.write(i, init.read(i));
			steady.sync();
		}
	}

	private static final class PeekMigrationInstruction implements Runnable {
		private final PeekableBuffer buffer;
		private final int itemsConsumedDuringInit;
		private PeekMigrationInstruction(Storage storage, PeekableBufferConcreteStorage steady) {
			this.buffer = steady.buffer();
			this.itemsConsumedDuringInit = steady.minReadIndex();
		}
		@Override
		public void run() {
			buffer.consume(itemsConsumedDuringInit);
		}
	}

	/**
	 * The X doesn't stand for anything.  I just needed a different name.
	 */
	private static final class XDrainInstruction implements DrainInstruction {
		private final Token token;
		private final ConcreteStorage[] storage;
		private final int[] storageSelector, index;
		private XDrainInstruction(Token token, List<Pair<ConcreteStorage, Integer>> reads) {
			this.token = token;
			Set<ConcreteStorage> set = new HashSet<>();
			for (Pair<ConcreteStorage, Integer> p : reads)
				set.add(p.first);
			this.storage = set.toArray(new ConcreteStorage[set.size()]);
			this.storageSelector = new int[reads.size()];
			this.index = new int[reads.size()];
			for (int i = 0; i < reads.size(); ++i) {
				storageSelector[i] = Arrays.asList(storage).indexOf(reads.get(i).first);
				index[i] = reads.get(i).second;
			}
		}
		@Override
		public Map<Token, Object[]> call() {
			Object[] data = new Object[index.length];
			int idx = 0;
			for (int i = 0; i < index.length; ++i)
				data[idx++] = storage[storageSelector[i]].read(index[i]);
			return ImmutableMap.of(token, data);
		}
	}

	/**
	 * PeekReadInstruction implements special handling for PeekableBuffer.
	 */
	private static final class PeekReadInstruction implements ReadInstruction {
		private final Token token;
		private final int count;
		private PeekableBuffer buffer;
		private PeekReadInstruction(TokenActor a, int count) {
			assert a.isInput() : a;
			this.token = a.token();
			//If we "read" count new items, the maximum available index is given
			//by the output index function.
			this.count = a.translateOutputIndex(0, count);
		}
		@Override
		public void init(Map<Token, Buffer> buffers) {
			if (!buffers.containsKey(token)) return;
			if (buffer != null)
				checkState(buffers.get(token) == buffer, "reassigning %s from %s to %s", token, buffer, buffers.get(token));
			this.buffer = (PeekableBuffer)buffers.get(token);
		}
		@Override
		public Map<Token, Integer> getMinimumBufferCapacity() {
			//This shouldn't matter because we've already created the buffers.
			return ImmutableMap.of(token, count);
		}
		@Override
		public boolean load() {
			//Ensure data is present for reading.
			return buffer.size() >= count;
		}
		@Override
		public Map<Token, Object[]> unload() {
			//Data is not consumed from the underlying buffer until it's
			//adjusted, so no work is necessary here.
			return ImmutableMap.of();
		}
	}

	private static final class BulkReadInstruction implements ReadInstruction {
		private final Token token;
		private final BulkWritableConcreteStorage storage;
		private final int index, count;
		private Buffer buffer;
		private BulkReadInstruction(TokenActor a, BulkWritableConcreteStorage storage, int count) {
			assert a.isInput() : a;
			this.token = a.token();
			this.storage = storage;
			this.index = a.translateOutputIndex(0, 0);
			this.count = count;
		}
		@Override
		public void init(Map<Token, Buffer> buffers) {
			if (!buffers.containsKey(token)) return;
			if (buffer != null)
				checkState(buffers.get(token) == buffer, "reassigning %s from %s to %s", token, buffer, buffers.get(token));
			this.buffer = buffers.get(token);
		}
		@Override
		public Map<Token, Integer> getMinimumBufferCapacity() {
			return ImmutableMap.of(token, count);
		}
		@Override
		public boolean load() {
			if (buffer.size() < count)
				return false;
			storage.bulkWrite(buffer, index, count);
			storage.sync();
			return true;
		}
		@Override
		public Map<Token, Object[]> unload() {
			Object[] data = new Object[count];
			for (int i = index; i < count; ++i) {
				data[i - index] = storage.read(i);
			}
			return ImmutableMap.of(token, data);
		}
	}

	/**
	 * TODO: consider using read/write handles instead of read(), write()?
	 */
	private static final class TokenReadInstruction implements ReadInstruction {
		private final Token token;
		private final MethodHandle idxFxn;
		private final ConcreteStorage storage;
		private final int count;
		private Buffer buffer;
		private TokenReadInstruction(TokenActor a, ConcreteStorage storage, int count) {
			assert a.isInput() : a;
			this.token = a.token();
			this.storage = storage;
			this.idxFxn = Iterables.getOnlyElement(a.outputIndexFunctions());
			this.count = count;
		}
		@Override
		public void init(Map<Token, Buffer> buffers) {
			if (!buffers.containsKey(token)) return;
			if (buffer != null)
				checkState(buffers.get(token) == buffer, "reassigning %s from %s to %s", token, buffer, buffers.get(token));
			this.buffer = buffers.get(token);
		}
		@Override
		public Map<Token, Integer> getMinimumBufferCapacity() {
			return ImmutableMap.of(token, count);
		}
		@Override
		public boolean load() {
			Object[] data = new Object[count];
			if (!buffer.readAll(data))
				return false;
			for (int i = 0; i < data.length; ++i) {
				int idx;
				try {
					idx = (int)idxFxn.invokeExact(i);
				} catch (Throwable ex) {
					throw new AssertionError("Can't happen! Index functions should not throw", ex);
				}
				storage.write(idx, data[i]);
			}
			storage.sync();
			return true;
		}
		@Override
		public Map<Token, Object[]> unload() {
			Object[] data = new Object[count];
			for (int i = 0; i < data.length; ++i) {
				int idx;
				try {
					idx = (int)idxFxn.invokeExact(i);
				} catch (Throwable ex) {
					throw new AssertionError("Can't happen! Index functions should not throw", ex);
				}
				data[i] = storage.read(idx);
			}
			return ImmutableMap.of(token, data);
		}
	}

	/**
	 * Doesn't read anything, but does respond to getMinimumBufferCapacity().
	 */
	private static final class NopReadInstruction implements ReadInstruction {
		private final Token token;
		private NopReadInstruction(Token token) {
			this.token = token;
		}
		@Override
		public void init(Map<Token, Buffer> buffers) {
		}
		@Override
		public Map<Token, Integer> getMinimumBufferCapacity() {
			return ImmutableMap.of(token, 0);
		}
		@Override
		public boolean load() {
			return true;
		}
		@Override
		public Map<Token, Object[]> unload() {
			return ImmutableMap.of();
		}
	}

	private static final class BulkWriteInstruction implements WriteInstruction {
		private final Token token;
		private final BulkReadableConcreteStorage storage;
		private final int index, count;
		private Buffer buffer;
		private int written;
		private BulkWriteInstruction(TokenActor a, BulkReadableConcreteStorage storage, int count) {
			assert a.isOutput(): a;
			this.token = a.token();
			this.storage = storage;
			this.index = a.translateInputIndex(0, 0);
			this.count = count;
		}
		@Override
		public void init(Map<Token, Buffer> buffers) {
			if (!buffers.containsKey(token)) return;
			if (buffer != null)
				checkState(buffers.get(token) == buffer, "reassigning %s from %s to %s", token, buffer, buffers.get(token));
			this.buffer = buffers.get(token);
		}
		@Override
		public Map<Token, Integer> getMinimumBufferCapacity() {
			return ImmutableMap.of(token, count);
		}
		@Override
		public Boolean call() {
			written += storage.bulkRead(buffer, index + written, count);
			if (written < count)
				return false;
			written = 0;
			return true;
		}
	}

	/**
	 * TODO: consider using read handles instead of read()?
	 */
	private static final class TokenWriteInstruction implements WriteInstruction {
		private final Token token;
		private final MethodHandle idxFxn;
		private final ConcreteStorage storage;
		private final int count;
		private Buffer buffer;
		private int written;
		private TokenWriteInstruction(TokenActor a, ConcreteStorage storage, int count) {
			assert a.isOutput() : a;
			this.token = a.token();
			this.storage = storage;
			this.idxFxn = Iterables.getOnlyElement(a.inputIndexFunctions());
			this.count = count;
		}
		@Override
		public void init(Map<Token, Buffer> buffers) {
			if (!buffers.containsKey(token)) return;
			if (buffer != null)
				checkState(buffers.get(token) == buffer, "reassigning %s from %s to %s", token, buffer, buffers.get(token));
			this.buffer = buffers.get(token);
		}
		@Override
		public Map<Token, Integer> getMinimumBufferCapacity() {
			return ImmutableMap.of(token, count);
		}
		@Override
		public Boolean call() {
			Object[] data = new Object[count];
			for (int i = 0; i < count; ++i) {
				int idx;
				try {
					idx = (int)idxFxn.invokeExact(i);
				} catch (Throwable ex) {
					throw new AssertionError("Can't happen! Index functions should not throw", ex);
				}
				data[i] = storage.read(idx);
			}
			written += buffer.write(data, written, data.length-written);
			if (written < count)
				return false;
			written = 0;
			return true;
		}
	}

	/**
	 * Doesn't write anything, but does respond to getMinimumBufferCapacity().
	 */
	private static final class NopWriteInstruction implements WriteInstruction {
		private final Token token;
		private NopWriteInstruction(Token token) {
			this.token = token;
		}
		@Override
		public void init(Map<Token, Buffer> buffers) {
		}
		@Override
		public Map<Token, Integer> getMinimumBufferCapacity() {
			return ImmutableMap.of(token, 0);
		}
		@Override
		public Boolean call() {
			return true;
		}
	}

	/**
	 * Writes initial data into init storage, or "unloads" it (just returning it
	 * as it was in the DrainData, not actually reading the storage) if we drain
	 * during init.  (Any remaining data after init will be migrated as normal.)
	 * There's only one of these per blob because it returns all the data, and
	 * it should be the first initReadInstruction.
	 */
	private static final class InitDataReadInstruction implements ReadInstruction {
		private final ImmutableMap<ConcreteStorage, ImmutableList<Pair<ImmutableList<Object>, MethodHandle>>> toWrite;
		private final ImmutableMap<Token, ImmutableList<Object>> initialStateDataMap;
		private InitDataReadInstruction(Map<Storage, ConcreteStorage> initStorage, ImmutableMap<Token, ImmutableList<Object>> initialStateDataMap) {
			ImmutableMap.Builder<ConcreteStorage, ImmutableList<Pair<ImmutableList<Object>, MethodHandle>>> toWriteBuilder = ImmutableMap.builder();
			for (Map.Entry<Storage, ConcreteStorage> e : initStorage.entrySet()) {
				Storage s = e.getKey();
				if (s.isInternal()) continue;
				if (s.initialData().isEmpty()) continue;
				toWriteBuilder.put(e.getValue(), ImmutableList.copyOf(s.initialData()));
			}
			this.toWrite = toWriteBuilder.build();
			this.initialStateDataMap = initialStateDataMap;
		}
		@Override
		public void init(Map<Token, Buffer> buffers) {
		}
		@Override
		public Map<Token, Integer> getMinimumBufferCapacity() {
			return ImmutableMap.of();
		}
		@Override
		public boolean load() {
			for (Map.Entry<ConcreteStorage, ImmutableList<Pair<ImmutableList<Object>, MethodHandle>>> e : toWrite.entrySet())
				for (Pair<ImmutableList<Object>, MethodHandle> p : e.getValue())
					for (int i = 0; i < p.first.size(); ++i) {
						int idx;
						try {
							idx = (int)p.second.invokeExact(i);
						} catch (Throwable ex) {
							throw new AssertionError("Can't happen! Index functions should not throw", ex);
						}
						e.getKey().write(idx, p.first.get(i));
					}
			return true;
		}
		@Override
		public Map<Token, Object[]> unload() {
			Map<Token, Object[]> r = new HashMap<>();
			for (Map.Entry<Token, ImmutableList<Object>> e : initialStateDataMap.entrySet())
				r.put(e.getKey(), e.getValue().toArray());
			return r;
		}
	}

	private static final class ReportThroughputInstruction implements ReadInstruction, WriteInstruction {
		private static final long WARMUP_NANOS = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
		private static final long TIMING_NANOS = TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS);
		private final long throughputPerSteadyState;
		private int steadyStates = 0;
		private long firstNanoTime = Long.MIN_VALUE, afterWarmupNanoTime = Long.MIN_VALUE;
		private ReportThroughputInstruction(long throughputPerSteadyState) {
			this.throughputPerSteadyState = throughputPerSteadyState;
		}
		@Override
		public void init(Map<Token, Buffer> buffers) {}
		@Override
		public Map<Token, Integer> getMinimumBufferCapacity() {
			return ImmutableMap.of();
		}
		@Override
		public Map<Token, Object[]> unload() {
			return ImmutableMap.of();
		}

		@Override
		public boolean load() {
			long currentTime = time();
			if (firstNanoTime == Long.MIN_VALUE)
				firstNanoTime = currentTime;
			else if (afterWarmupNanoTime == Long.MIN_VALUE && currentTime - firstNanoTime > WARMUP_NANOS)
				afterWarmupNanoTime = currentTime;
			return true;
		}
		@Override
		public Boolean call() {
			if (afterWarmupNanoTime != Long.MIN_VALUE) {
				++steadyStates;
				long currentTime = time();
				long elapsed = currentTime - afterWarmupNanoTime;
				if (elapsed > TIMING_NANOS) {
					long itemsOutput = steadyStates * throughputPerSteadyState;
					System.out.format("%d/%d/%d/%d#%n", steadyStates, itemsOutput, elapsed, elapsed/itemsOutput);
					System.out.flush();
					System.exit(0);
				}
			}
			return true;
		}
		private static long time() {
//			return System.currentTimeMillis()*1000000;
			return System.nanoTime();
		}
	}

	/**
	 * Creates the blob host.  This mostly involves shuffling our state into the
	 * form the blob host wants.
	 * @return the blob
	 */
	public Blob instantiateBlob() {
		ImmutableSortedSet.Builder<Token> inputTokens = ImmutableSortedSet.naturalOrder(),
				outputTokens = ImmutableSortedSet.naturalOrder();
		for (TokenActor ta : Iterables.filter(actors, TokenActor.class))
			(ta.isInput() ? inputTokens : outputTokens).add(ta.token());
		ImmutableList.Builder<MethodHandle> storageAdjusts = ImmutableList.builder();
		for (ConcreteStorage s : steadyStateStorage.values())
			storageAdjusts.add(s.adjustHandle());
		return new Compiler2BlobHost(workers, config,
				inputTokens.build(), outputTokens.build(),
				initCode, steadyStateCode,
				storageAdjusts.build(),
				initReadInstructions, initWriteInstructions, migrationInstructions,
				readInstructions, writeInstructions, drainInstructions,
				precreatedBuffers);
	}

	public static void main(String[] args) {
		StreamCompiler sc;
		Benchmark bm;
		if (args.length == 3) {
			String benchmarkName = args[0];
			int cores = Integer.parseInt(args[1]);
			int multiplier = Integer.parseInt(args[2]);
			sc = new Compiler2StreamCompiler().maxNumCores(cores).multiplier(multiplier);
			bm = Benchmarker.getBenchmarkByName(benchmarkName);
		} else {
			sc = new Compiler2StreamCompiler().multiplier(384).maxNumCores(4);
			bm = new FMRadio.FMRadioBenchmarkProvider().iterator().next();
		}
		Benchmarker.runBenchmark(bm, sc).get(0).print(System.out);
	}

	private void printDot(String stage) {
		try (FileWriter fw = new FileWriter(stage+".dot");
				BufferedWriter bw = new BufferedWriter(fw)) {
			bw.write("digraph {\n");
			for (ActorGroup g : groups) {
				bw.write("subgraph cluster_"+Integer.toString(g.id()).replace('-', '_')+" {\n");
				for (Actor a : g.actors())
					bw.write(String.format("\"%s\";\n", nodeName(a)));
				bw.write("label = \"x"+externalSchedule.get(g)+"\";\n");
				bw.write("}\n");
			}
			for (ActorGroup g : groups) {
				for (Actor a : g.actors()) {
					for (Storage s : a.outputs())
						for (Actor b : s.downstream())
							bw.write(String.format("\"%s\" -> \"%s\";\n", nodeName(a), nodeName(b)));
				}
			}
			bw.write("}\n");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	private String nodeName(Actor a) {
		if (a instanceof TokenActor)
			return (((TokenActor)a).isInput()) ? "input" : "output";
		WorkerActor wa = (WorkerActor)a;
		String workerClassName = wa.worker().getClass().getSimpleName();
		return workerClassName.replaceAll("[a-z]", "") + "@" + Integer.toString(wa.id()).replace('-', '_');
	}
}
