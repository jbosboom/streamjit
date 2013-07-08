package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.IOInfo;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.compiler.insts.ArrayLoadInst;
import edu.mit.streamjit.impl.compiler.insts.ArrayStoreInst;
import edu.mit.streamjit.impl.compiler.insts.BinaryInst;
import edu.mit.streamjit.impl.compiler.insts.CallInst;
import edu.mit.streamjit.impl.compiler.insts.CastInst;
import edu.mit.streamjit.impl.compiler.insts.Instruction;
import edu.mit.streamjit.impl.compiler.insts.JumpInst;
import edu.mit.streamjit.impl.compiler.insts.LoadInst;
import edu.mit.streamjit.impl.compiler.insts.NewArrayInst;
import edu.mit.streamjit.impl.compiler.insts.ReturnInst;
import edu.mit.streamjit.impl.compiler.insts.StoreInst;
import edu.mit.streamjit.impl.compiler.types.FieldType;
import edu.mit.streamjit.impl.compiler.types.MethodType;
import edu.mit.streamjit.impl.compiler.types.RegularType;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.impl.interp.ChannelFactory;
import edu.mit.streamjit.impl.interp.EmptyChannel;
import edu.mit.streamjit.util.Pair;
import edu.mit.streamjit.util.TopologicalSort;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/24/2013
 */
public final class Compiler {
	/**
	 * A counter used to generate package names unique to a given machine.
	 */
	private static final AtomicInteger PACKAGE_NUMBER = new AtomicInteger();
	private final Set<Worker<?, ?>> workers;
	private final Configuration config;
	private final int maxNumCores;
	private final ImmutableSet<IOInfo> ioinfo;
	private final Worker<?, ?> firstWorker, lastWorker;
	/**
	 * Maps a worker to the StreamNode that contains it.  Updated by
	 * StreamNode's constructors.  (It would be static in
	 * StreamNode if statics of inner classes were supported and worked as
	 * though there was one instance per parent instance.)
	 */
	private final Map<Worker<?, ?>, StreamNode> streamNodes = new IdentityHashMap<>();
	private final Map<Worker<?, ?>, Method> workerWorkMethods = new IdentityHashMap<>();
	private ImmutableMap<StreamNode, Integer> schedule;
	private ImmutableMap<Worker<?, ?>, Integer> initSchedule;
	private final String packagePrefix;
	private final Module module = new Module();
	private final Klass blobKlass;
	/**
	 * The steady-state execution multiplier (the number of executions to run
	 * per synchronization).
	 */
	private final int multiplier;
	/**
	 * The work method type, which is void(Object[][], int[], int[], Object[][],
	 * int[], int[]). (There is no receiver argument.)
	 */
	private final MethodType workMethodType;
	private ImmutableMap<Token, BufferData> buffers;
	public Compiler(Set<Worker<?, ?>> workers, Configuration config, int maxNumCores) {
		this.workers = workers;
		this.config = config;
		this.maxNumCores = maxNumCores;
		this.ioinfo = IOInfo.create(workers);

		//We can only have one first and last worker, though they can have
		//multiple inputs/outputs.
		Worker<?, ?> firstWorker = null, lastWorker = null;
		for (IOInfo io : ioinfo)
			if (io.isInput())
				if (firstWorker == null)
					firstWorker = io.downstream();
				else
					checkArgument(firstWorker == io.downstream(), "two input workers");
			else
				if (lastWorker == null)
					lastWorker = io.upstream();
				else
					checkArgument(lastWorker == io.upstream(), "two output workers");
		assert firstWorker != null : "Can't happen! No first worker?";
		assert lastWorker != null : "Can't happen! No last worker?";
		this.firstWorker = firstWorker;
		this.lastWorker = lastWorker;

		//We require that all rates of workers in our set are fixed, except for
		//the output rates of the last worker.
		for (Worker<?, ?> w : workers) {
			for (Rate r : w.getPeekRates())
				checkArgument(r.isFixed());
			for (Rate r : w.getPopRates())
				checkArgument(r.isFixed());
			if (w != lastWorker)
				for (Rate r : w.getPushRates())
					checkArgument(r.isFixed());
		}

		//We don't support messaging.
		List<MessageConstraint> constraints = MessageConstraint.findConstraints(firstWorker);
		for (MessageConstraint c : constraints) {
			checkArgument(!workers.contains(c.getSender()));
			checkArgument(!workers.contains(c.getRecipient()));
		}

		this.packagePrefix = "compiler"+PACKAGE_NUMBER.getAndIncrement()+".";
		this.blobKlass = new Klass(packagePrefix + "Blob",
				module.getKlass(Object.class),
				Collections.singletonList(module.getKlass(Blob.class)),
				module);
		this.multiplier = config.getParameter("multiplier", Configuration.IntParameter.class).getValue();
		this.workMethodType = module.types().getMethodType(void.class, Object[][].class, int[].class, int[].class, Object[][].class, int[].class, int[].class);
	}

	public Blob compile() {
		for (Worker<?, ?> w : workers)
			new StreamNode(w); //adds itself to streamNodes map
		fuse();
		//Compute per-node steady state execution counts.
		for (StreamNode n : ImmutableSet.copyOf(streamNodes.values()))
			n.internalSchedule();
		externalSchedule();
		allocateCores();
		declareBuffers();
		computeInitSchedule();
		//We generate a work method for each worker (which may result in
		//duplicates, but is required in general to handle worker fields), then
		//generate core code that stitches them together and does any
		//required data movement.
		for (Worker<?, ?> w : streamNodes.keySet())
			makeWorkMethod(w);
		for (StreamNode n : ImmutableSet.copyOf(streamNodes.values()))
			n.makeWorkMethod();
		generateCoreCode();
		addBlobPlumbing();
		blobKlass.dump(new PrintWriter(System.out, true));
		return instantiateBlob();
	}

	/**
	 * Fuses StreamNodes as directed by the configuration.
	 */
	private void fuse() {
		//TODO: some kind of worklist algorithm that fuses until no more fusion
		//possible, to handle state, peeking, or attempts to fuse with more than
		//one predecessor.
	}

	/**
	 * Allocates StreamNodes to cores as directed by the configuration, possibly
	 * fissing them (if assigning one node to multiple cores).
	 * TODO: do we want to permit unequal fiss allocations (e.g., 6 SSEs here,
	 * 3 SSEs there)?
	 */
	private void allocateCores() {
		//TODO
		//Note that any node containing a splitter or joiner can only go on one
		//core (as it has to synchronize for its inputs and outputs).
		//For now, just put everything on core 0.
		for (StreamNode n : ImmutableSet.copyOf(streamNodes.values()))
			n.cores.add(0);
	}

	/**
	 * Computes buffer capacity and initial sizes, declaring (but not
	 * arranging for initialization of) the blob class fields pointing to the
	 * buffers.
	 */
	private void declareBuffers() {
		ImmutableMap.Builder<Token, BufferData> builder = ImmutableMap.<Token, BufferData>builder();
		for (Pair<Worker<?, ?>, Worker<?, ?>> p : allWorkerPairsInBlob())
			//Only declare buffers for worker pairs not in the same node.  If
			//a node needs internal buffering, it handles that itself.  (This
			//implies that peeking filters cannot be fused upwards, but that's
			//a bad idea anyway.)
			if (!streamNodes.get(p.first).equals(streamNodes.get(p.second)))
				builder.put(new Token(p.first, p.second), new BufferData(p.first, p.second));
		//Make buffers for the inputs and outputs of this blob (which may or
		//may not be overall inputs of the stream graph).
		for (IOInfo info : ioinfo)
			if (firstWorker.equals(info.downstream()) || lastWorker.equals(info.upstream()))
				builder.put(info.token(), new BufferData(info.upstream(), info.downstream()));
		buffers = builder.build();
	}

	/**
	 * Computes the initialization schedule using the scheduler.
	 */
	private void computeInitSchedule() {
		ImmutableList.Builder<Scheduler.Channel<Worker<?, ?>>> builder = ImmutableList.<Scheduler.Channel<Worker<?, ?>>>builder();
		for (Pair<Worker<?, ?>, Worker<?, ?>> p : allWorkerPairsInBlob()) {
			int i = Workers.getSuccessors(p.first).indexOf(p.second);
			int j = Workers.getPredecessors(p.second).indexOf(p.first);
			int pushRate = p.first.getPushRates().get(i).max();
			int popRate = p.second.getPopRates().get(j).max();
			builder.add(new Scheduler.Channel<>(p.first, p.second, pushRate, popRate, buffers.get(new Token(p.first, p.second)).initialSize));
		}
		initSchedule = Scheduler.schedule(builder.build());
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private ImmutableList<Pair<Worker<?, ?>, Worker<?, ?>>> allWorkerPairsInBlob() {
		ImmutableList.Builder<Pair<Worker<?, ?>, Worker<?, ?>>> builder = ImmutableList.<Pair<Worker<?, ?>, Worker<?, ?>>>builder();
		for (Worker<?, ?> u : workers)
			for (Worker<?, ?> d : Workers.getSuccessors(u))
				if (workers.contains(d))
					builder.add(new Pair(u, d));
		return builder.build();
	}

	/**
	 * Make the work method for the given worker.  We actually make two methods
	 * here: first we make a copy with a dummy receiver argument, just to have a
	 * copy to work with.  After remapping every use of that receiver (remapping
	 * field accesses to the worker's static fields, remapping JIT-hooks to
	 * their implementations, and remapping utility methods in the worker class
	 * recursively), we then create the actual work method without the receiver
	 * argument.
	 * @param worker
	 */
	private void makeWorkMethod(Worker<?, ?> worker) {
		StreamNode node = streamNodes.get(worker);
		int id = Workers.getIdentifier(worker);
		int numInputs = getNumInputs(worker);
		int numOutputs = getNumOutputs(worker);
		Klass workerKlass = module.getKlass(worker.getClass());
		Method oldWork = workerKlass.getMethod("work", module.types().getMethodType(void.class, worker.getClass()));
		oldWork.resolve();

		//Add a dummy receiver argument so we can clone the user's work method.
		MethodType rworkMethodType = workMethodType.prependArgument(module.types().getRegularType(workerKlass));
		Method newWork = new Method("rwork"+id, rworkMethodType, EnumSet.of(Modifier.PRIVATE, Modifier.STATIC), blobKlass);
		newWork.arguments().get(0).setName("dummyReceiver");
		newWork.arguments().get(1).setName("ichannels");
		newWork.arguments().get(2).setName("ioffsets");
		newWork.arguments().get(3).setName("iincrements");
		newWork.arguments().get(4).setName("ochannels");
		newWork.arguments().get(5).setName("ooffsets");
		newWork.arguments().get(6).setName("oincrements");

		Map<Value, Value> vmap = new IdentityHashMap<>();
		vmap.put(oldWork.arguments().get(0), newWork.arguments().get(0));
		Cloning.cloneMethod(oldWork, newWork, vmap);

		BasicBlock entryBlock = new BasicBlock(module, "entry");
		newWork.basicBlocks().add(0, entryBlock);

		//We make copies of the offset arrays.  (int[].clone() returns Object,
		//so we have to cast.)
		Method clone = Iterables.getOnlyElement(module.getKlass(Object.class).getMethods("clone"));
		CallInst ioffsetCloneCall = new CallInst(clone, newWork.arguments().get(2));
		entryBlock.instructions().add(ioffsetCloneCall);
		CastInst ioffsetCast = new CastInst(module.types().getArrayType(int[].class), ioffsetCloneCall);
		entryBlock.instructions().add(ioffsetCast);
		LocalVariable ioffsetCopy = new LocalVariable((RegularType)ioffsetCast.getType(), "ioffsetCopy", newWork);
		StoreInst popCountInit = new StoreInst(ioffsetCopy, ioffsetCast);
		popCountInit.setName("ioffsetInit");
		entryBlock.instructions().add(popCountInit);

		CallInst ooffsetCloneCall = new CallInst(clone, newWork.arguments().get(5));
		entryBlock.instructions().add(ooffsetCloneCall);
		CastInst ooffsetCast = new CastInst(module.types().getArrayType(int[].class), ooffsetCloneCall);
		entryBlock.instructions().add(ooffsetCast);
		LocalVariable ooffsetCopy = new LocalVariable((RegularType)ooffsetCast.getType(), "ooffsetCopy", newWork);
		StoreInst pushCountInit = new StoreInst(ooffsetCopy, ooffsetCast);
		pushCountInit.setName("ooffsetInit");
		entryBlock.instructions().add(pushCountInit);

		entryBlock.instructions().add(new JumpInst(newWork.basicBlocks().get(1)));

		//Remap stuff in rwork.
		for (BasicBlock b : newWork.basicBlocks())
			for (Instruction i : ImmutableList.copyOf(b.instructions()))
				if (Iterables.contains(i.operands(), newWork.arguments().get(0)))
					remapEliminiatingReceiver(i, worker);

		//At this point, we've replaced all uses of the dummy receiver argument.
		assert newWork.arguments().get(0).uses().isEmpty();
		Method trueWork = new Method("work"+id, workMethodType, EnumSet.of(Modifier.PRIVATE, Modifier.STATIC), blobKlass);
		vmap.clear();
		vmap.put(newWork.arguments().get(0), null);
		for (int i = 1; i < newWork.arguments().size(); ++i)
			vmap.put(newWork.arguments().get(i), trueWork.arguments().get(i-1));
		Cloning.cloneMethod(newWork, trueWork, vmap);
		workerWorkMethods.put(worker, trueWork);
		newWork.eraseFromParent();
	}

	private void remapEliminiatingReceiver(Instruction inst, Worker<?, ?> worker) {
		BasicBlock block = inst.getParent();
		Method rwork = inst.getParent().getParent();
		if (inst instanceof CallInst) {
			CallInst ci = (CallInst)inst;
			Method method = ci.getMethod();
			Klass filterKlass = module.getKlass(Filter.class);
			Klass splitterKlass = module.getKlass(Splitter.class);
			Klass joinerKlass = module.getKlass(Joiner.class);
			Method pop1Filter = filterKlass.getMethod("pop", module.types().getMethodType(Object.class, Filter.class));
			assert pop1Filter != null;
			Method pop1Splitter = splitterKlass.getMethod("pop", module.types().getMethodType(Object.class, Splitter.class));
			assert pop1Splitter != null;
			Method push1Filter = filterKlass.getMethod("push", module.types().getMethodType(void.class, Filter.class, Object.class));
			assert push1Filter != null;
			Method push1Joiner = joinerKlass.getMethod("push", module.types().getMethodType(void.class, Joiner.class, Object.class));
			assert push1Joiner != null;
			Method pop2 = joinerKlass.getMethod("pop", module.types().getMethodType(Object.class, Joiner.class, int.class));
			assert pop2 != null;
			Method push2 = splitterKlass.getMethod("push", module.types().getMethodType(void.class, Splitter.class, int.class, Object.class));
			assert push2 != null;
			Method inputs = joinerKlass.getMethod("inputs", module.types().getMethodType(int.class, Joiner.class));
			assert inputs != null;
			Method outputs = splitterKlass.getMethod("outputs", module.types().getMethodType(int.class, Splitter.class));
			assert outputs != null;

			Method channelPush = module.getKlass(Channel.class).getMethod("push", module.types().getMethodType(void.class, Channel.class, Object.class));
			assert channelPush != null;

			if (method.equals(pop1Filter) || method.equals(pop1Splitter) || method.equals(pop2)) {
				Value channelNumber = method.equals(pop2) ? ci.getArgument(1) : module.constants().getSmallestIntConstant(0);
				Argument ichannels = rwork.getArgument("ichannels");
				ArrayLoadInst channel = new ArrayLoadInst(ichannels, channelNumber);
				LoadInst ioffsets = new LoadInst(rwork.getLocalVariable("ioffsetCopy"));
				ArrayLoadInst offset = new ArrayLoadInst(ioffsets, channelNumber);
				ArrayLoadInst item = new ArrayLoadInst(channel, offset);
				item.setName("poppedItem");

				Argument iincrements = rwork.getArgument("iincrements");
				ArrayLoadInst increment = new ArrayLoadInst(iincrements, channelNumber);
				BinaryInst newOffset = new BinaryInst(offset, BinaryInst.Operation.ADD, increment);
				ArrayStoreInst storeNewOffset = new ArrayStoreInst(ioffsets, channelNumber, newOffset);
				inst.replaceInstWithInsts(item, channel, ioffsets, offset, item, increment, newOffset, storeNewOffset);
			} else if ((method.equals(push1Filter) || method.equals(push1Joiner)) || method.equals(push2)) {
				Value channelNumber = method.equals(push2) ? ci.getArgument(1) : module.constants().getSmallestIntConstant(0);
				Value item = method.equals(push2) ? ci.getArgument(2) : ci.getArgument(1);
				Argument ochannels = rwork.getArgument("ochannels");
				ArrayLoadInst channel = new ArrayLoadInst(ochannels, channelNumber);
				LoadInst ooffsets = new LoadInst(rwork.getLocalVariable("ooffsetCopy"));
				ArrayLoadInst offset = new ArrayLoadInst(ooffsets, channelNumber);
				ArrayStoreInst store = new ArrayStoreInst(channel, offset, item);

				Argument oincrements = rwork.getArgument("oincrements");
				ArrayLoadInst increment = new ArrayLoadInst(oincrements, channelNumber);
				BinaryInst newOffset = new BinaryInst(offset, BinaryInst.Operation.ADD, increment);
				ArrayStoreInst storeNewOffset = new ArrayStoreInst(ooffsets, channelNumber, newOffset);
				inst.replaceInstWithInsts(store, channel, ooffsets, offset, store, increment, newOffset, storeNewOffset);
			} else if (method.equals(outputs)) {
				inst.replaceInstWithValue(module.constants().getSmallestIntConstant(getNumOutputs(worker)));
			} else if (method.equals(inputs)) {
				inst.replaceInstWithValue(module.constants().getSmallestIntConstant(getNumInputs(worker)));
			} else
				throw new AssertionError(inst);
		} else if (inst instanceof LoadInst) {
			LoadInst li = (LoadInst)inst;
			assert li.getLocation() instanceof Field;
			LoadInst replacement = new LoadInst(streamNodes.get(worker).fields.get(worker, (Field)li.getLocation()));
			li.replaceInstWithInst(replacement);
		} else
			throw new AssertionError("Couldn't eliminate reciever: "+inst);
	}

	private int getNumInputs(Worker<?, ?> w) {
		return Workers.getInputChannels(w).size();
	}

	private int getNumOutputs(Worker<?, ?> w) {
		return Workers.getOutputChannels(w).size();
	}

	private void generateCoreCode() {

	}

	/**
	 * Adds required plumbing code to the blob class, such as the ctor and the
	 * implementations of the Blob methods.
	 */
	private void addBlobPlumbing() {
		//ctor
		Method init = new Method("<init>",
				module.types().getMethodType(module.types().getType(blobKlass)),
				EnumSet.noneOf(Modifier.class),
				blobKlass);
		BasicBlock b = new BasicBlock(module);
		init.basicBlocks().add(b);
		Method objCtor = module.getKlass(Object.class).getMethods("<init>").iterator().next();
		b.instructions().add(new CallInst(objCtor));
		b.instructions().add(new ReturnInst(module.types().getVoidType()));
		//TODO: other Blob interface methods
	}

	private Blob instantiateBlob() {
		ModuleClassLoader mcl = new ModuleClassLoader(module);
		try {
			Class<?> blobClass = mcl.loadClass(blobKlass.getName());
			Constructor<?> ctor = blobClass.getDeclaredConstructor();
			ctor.setAccessible(true);
			return (Blob)ctor.newInstance();
		} catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | IllegalAccessException | InstantiationException ex) {
			throw new AssertionError(ex);
		}
	}

	private void externalSchedule() {
		ImmutableSet<StreamNode> nodes = ImmutableSet.copyOf(streamNodes.values());
		ImmutableList.Builder<Scheduler.Channel<StreamNode>> channels = ImmutableList.<Scheduler.Channel<StreamNode>>builder();
		for (StreamNode a : nodes)
			for (StreamNode b : nodes)
				channels.addAll(a.findChannels(b));
		schedule = Scheduler.schedule(channels.build());
		System.out.println(schedule);
	}

	private final class StreamNode {
		private final int id;
		private final ImmutableSet<Worker<?, ?>> workers;
		private final ImmutableSortedSet<IOInfo> ioinfo;
		private ImmutableMap<Worker<?, ?>, ImmutableSortedSet<IOInfo>> inputIOs, outputIOs;
		/**
		 * The number of individual worker executions per steady-state execution
		 * of the StreamNode.
		 */
		private ImmutableMap<Worker<?, ?>, Integer> execsPerNodeExec;
		/**
		 * This node's work method.  May be null if the method hasn't been
		 * created yet.  TODO: if we put multiplicities inside work methods,
		 * we'll need one per core.  Alternately we could put them outside and
		 * inline/specialize as a postprocessing step.
		 */
		private Method workMethod;
		/**
		 * Maps each worker's fields to the corresponding fields in the blob
		 * class.
		 */
		private final Table<Worker<?, ?>, Field, Field> fields = HashBasedTable.create();
		/**
		 * Maps each worker's fields to the actual values of those fields.
		 */
		private final Table<Worker<?, ?>, Field, Object> fieldValues = HashBasedTable.create();
		private final List<Integer> cores = new ArrayList<>();

		private StreamNode(Worker<?, ?> worker) {
			this.id = Workers.getIdentifier(worker);
			this.workers = (ImmutableSet<Worker<?, ?>>)ImmutableSet.of(worker);
			this.ioinfo = ImmutableSortedSet.copyOf(IOInfo.TOKEN_SORT, IOInfo.create(workers));
			buildWorkerData(worker);

			assert !streamNodes.containsKey(worker);
			streamNodes.put(worker, this);
		}

		/**
		 * Fuses two StreamNodes.  They should not yet have been scheduled or
		 * had work functions constructed.
		 */
		private StreamNode(StreamNode a, StreamNode b) {
			this.id = Math.min(a.id, b.id);
			this.workers = ImmutableSet.<Worker<?, ?>>builder().addAll(a.workers).addAll(b.workers).build();
			this.ioinfo = ImmutableSortedSet.copyOf(IOInfo.TOKEN_SORT, IOInfo.create(workers));
			this.fields.putAll(a.fields);
			this.fields.putAll(b.fields);
			this.fieldValues.putAll(a.fieldValues);
			this.fieldValues.putAll(b.fieldValues);

			for (Worker<?, ?> w : a.workers)
				streamNodes.put(w, this);
			for (Worker<?, ?> w : b.workers)
				streamNodes.put(w, this);
		}

		/**
		 * Compute the steady-state multiplicities of each worker in this node
		 * for each execution of the node.
		 */
		public void internalSchedule() {
			if (workers.size() == 1) {
				this.execsPerNodeExec = ImmutableMap.<Worker<?, ?>, Integer>builder().put(workers.iterator().next(), 1).build();
				return;
			}

			//Find all the channels within this StreamNode.
			List<Scheduler.Channel<Worker<?, ?>>> channels = new ArrayList<>();
			for (Worker<?, ?> w : workers) {
				@SuppressWarnings("unchecked")
				List<Worker<?, ?>> succs = (List<Worker<?, ?>>)Workers.getSuccessors(w);
				for (int i = 0; i < succs.size(); ++i) {
					Worker<?, ?> s = succs.get(i);
					if (workers.contains(s)) {
						int j = Workers.getPredecessors(s).indexOf(w);
						assert j != -1;
						channels.add(new Scheduler.Channel<>(w, s, w.getPushRates().get(i).max(), s.getPopRates().get(j).max()));
					}
				}
			}
			this.execsPerNodeExec = Scheduler.schedule(channels);
		}

		/**
		 * Returns a list of scheduler channels from this node to the given
		 * node, with rates corrected for the internal schedule for each node.
		 */
		public List<Scheduler.Channel<StreamNode>> findChannels(StreamNode other) {
			ImmutableList.Builder<Scheduler.Channel<StreamNode>> retval = ImmutableList.<Scheduler.Channel<StreamNode>>builder();
			for (IOInfo info : ioinfo) {
				if (info.isOutput() && other.workers.contains(info.downstream())) {
					int i = Workers.getSuccessors(info.upstream()).indexOf(info.downstream());
					assert i != -1;
					int j = Workers.getPredecessors(info.downstream()).indexOf(info.upstream());
					assert j != -1;
					retval.add(new Scheduler.Channel<>(this, other,
							info.upstream().getPushRates().get(i).max() * execsPerNodeExec.get(info.upstream()),
							info.downstream().getPopRates().get(j).max() * other.execsPerNodeExec.get(info.downstream())));
				}
			}
			return retval.build();
		}

		private void buildWorkerData(Worker<?, ?> worker) {
			Klass workerKlass = module.getKlass(worker.getClass());

			//Build the new fields.
			for (Field f : workerKlass.fields()) {
				java.lang.reflect.Field rf = f.getBackingField();
				Set<Modifier> modifiers = EnumSet.of(Modifier.PRIVATE, Modifier.STATIC);
				//We can make the new field final if the original field is final or
				//if the worker isn't stateful.
				if (f.modifiers().contains(Modifier.FINAL) || !(worker instanceof StatefulFilter))
					modifiers.add(Modifier.FINAL);

				Field nf = new Field(f.getType().getFieldType(),
						"w" + id + "$" + f.getName(),
						modifiers,
						blobKlass);
				fields.put(worker, f, nf);

				try {
					rf.setAccessible(true);
					Object value = rf.get(worker);
					fieldValues.put(worker, f, value);
				} catch (IllegalAccessException ex) {
					//Either setAccessible will succeed or we'll throw a
					//SecurityException, so we'll never get here.
					throw new AssertionError("Can't happen!", ex);
				}
			}
		}

		private void makeWorkMethod() {
			assert workMethod == null : "remaking node work method";
			mapIOInfo();
			MethodType nodeWorkMethodType = module.types().getMethodType(module.types().getVoidType(), module.types().getRegularType(int.class));
			workMethod = new Method("nodework"+this.id, nodeWorkMethodType, EnumSet.of(Modifier.PRIVATE, Modifier.STATIC), blobKlass);
			Argument multiple = Iterables.getOnlyElement(workMethod.arguments());
			multiple.setName("multiple");
			BasicBlock entryBlock = new BasicBlock(module, "entry");
			workMethod.basicBlocks().add(entryBlock);

			Map<Token, Value> localBuffers = new HashMap<>();
			ImmutableList<Worker<?, ?>> orderedWorkers = TopologicalSort.sort(new ArrayList<>(workers), new TopologicalSort.PartialOrder<Worker<?, ?>>() {
				@Override
				public boolean lessThan(Worker<?, ?> a, Worker<?, ?> b) {
					return Workers.getAllSuccessors(a).contains(b);
				}
			});
			for (Worker<?, ?> w : orderedWorkers) {
				//Input buffers
				List<Worker<?, ?>> preds = (List<Worker<?, ?>>)Workers.getPredecessors(w);
				List<Value> ichannels;
				List<Value> ioffsets = new ArrayList<>();
				if (preds.isEmpty()) {
					ichannels = ImmutableList.<Value>of(buffers.get(Token.createOverallInputToken(w)).fields.get(0));
					int r = w.getPopRates().get(0).max() * execsPerNodeExec.get(w);
					BinaryInst offset = new BinaryInst(multiple, BinaryInst.Operation.MUL, module.constants().getConstant(r));
					offset.setName("ioffset0");
					entryBlock.instructions().add(offset);
					ioffsets.add(offset);
				} else {
					ichannels = new ArrayList<>(preds.size());
					for (int chanIdx = 0; chanIdx < preds.size(); ++chanIdx) {
						Worker<?, ?> p = preds.get(chanIdx);
						BufferData bufferData = buffers.get(new Token(p, w));
						if (workers.contains(p)) {
							assert bufferData == null : "BufferData created for internal buffer";
							Value localBuffer = localBuffers.get(new Token(p, w));
							assert localBuffer != null : "Local buffer needed before created";
							ichannels.add(localBuffer);
							ioffsets.add(module.constants().getConstant(0));
						} else {
							ichannels.add(bufferData.fields.get(0));
							int r = w.getPopRates().get(chanIdx).max() * execsPerNodeExec.get(w);
							BinaryInst offset = new BinaryInst(multiple, BinaryInst.Operation.MUL, module.constants().getConstant(r));
							offset.setName("ioffset"+chanIdx);
							entryBlock.instructions().add(offset);
							ioffsets.add(offset);
						}
					}
				}

				Pair<Value, List<Instruction>> ichannelArray = createChannelArray(ichannels);
				entryBlock.instructions().addAll(ichannelArray.second);
				Pair<Value, List<Instruction>> ioffsetArray = createIntArray(ioffsets);
				entryBlock.instructions().addAll(ioffsetArray.second);
				Pair<Value, List<Instruction>> iincrementArray = createIntArray(Collections.<Value>nCopies(ioffsets.size(), module.constants().getConstant(1)));
				entryBlock.instructions().addAll(iincrementArray.second);

				//Output buffers
				List<Worker<?, ?>> succs = (List<Worker<?, ?>>)Workers.getSuccessors(w);
				List<Value> ochannels;
				List<Value> ooffsets = new ArrayList<>();
				if (succs.isEmpty()) {
					ochannels = ImmutableList.<Value>of(buffers.get(Token.createOverallOutputToken(w)).fields.get(0));
					int r = w.getPushRates().get(0).max() * execsPerNodeExec.get(w);
					BinaryInst offset = new BinaryInst(multiple, BinaryInst.Operation.MUL, module.constants().getConstant(r));
					offset.setName("ooffset0");
					entryBlock.instructions().add(offset);
					ooffsets.add(offset);
				} else {
					ochannels = new ArrayList<>(preds.size());
					for (int chanIdx = 0; chanIdx < succs.size(); ++chanIdx) {
						Worker<?, ?> s = succs.get(chanIdx);
						BufferData bufferData = buffers.get(new Token(w, s));
						if (workers.contains(s)) {
							assert bufferData == null : "BufferData created for internal buffer";
							Value localBuffer = localBuffers.get(new Token(w, s));
							assert localBuffer != null : "Local buffer needed before created";
							ochannels.add(localBuffer);
							ooffsets.add(module.constants().getConstant(0));
						} else {
							ochannels.add(bufferData.fields.get(1));
							int r = w.getPushRates().get(chanIdx).max() * execsPerNodeExec.get(w);
							BinaryInst offset = new BinaryInst(multiple, BinaryInst.Operation.MUL, module.constants().getConstant(r));
							offset.setName("ooffset"+chanIdx);
							entryBlock.instructions().add(offset);
							ooffsets.add(offset);
						}
					}
				}

				Pair<Value, List<Instruction>> ochannelArray = createChannelArray(ochannels);
				entryBlock.instructions().addAll(ochannelArray.second);
				Pair<Value, List<Instruction>> ooffsetArray = createIntArray(ooffsets);
				entryBlock.instructions().addAll(ooffsetArray.second);
				Pair<Value, List<Instruction>> oincrementArray = createIntArray(Collections.<Value>nCopies(ooffsets.size(), module.constants().getConstant(1)));
				entryBlock.instructions().addAll(oincrementArray.second);

				for (int i = 0; i < execsPerNodeExec.get(w); ++i) {
					CallInst ci = new CallInst(workerWorkMethods.get(w), ichannelArray.first, ioffsetArray.first, iincrementArray.first, ochannelArray.first, ooffsetArray.first, oincrementArray.first);
					entryBlock.instructions().add(ci);
				}
			}
			entryBlock.instructions().add(new ReturnInst(module.types().getVoidType()));
		}

		private Pair<Value, List<Instruction>> createChannelArray(List<Value> channels) {
			ImmutableList.Builder<Instruction> insts = ImmutableList.builder();
			NewArrayInst nai = new NewArrayInst(module.types().getArrayType(Object[][].class), module.constants().getConstant(channels.size()));
			insts.add(nai);
			for (int i = 0; i < channels.size(); ++i) {
				Value toStore = channels.get(i);
				//If the value is a field, load it first.
				if (toStore.getType() instanceof FieldType) {
					LoadInst li = new LoadInst((Field)toStore);
					insts.add(li);
					toStore = li;
				}
				ArrayStoreInst asi = new ArrayStoreInst(nai, module.constants().getConstant(i), toStore);
				insts.add(asi);
			}
			return new Pair<Value, List<Instruction>>(nai, insts.build());
		}
		private Pair<Value, List<Instruction>> createIntArray(List<Value> ints) {
			ImmutableList.Builder<Instruction> insts = ImmutableList.builder();
			NewArrayInst nai = new NewArrayInst(module.types().getArrayType(int[].class), module.constants().getConstant(ints.size()));
			insts.add(nai);
			for (int i = 0; i < ints.size(); ++i) {
				Value toStore = ints.get(i);
				ArrayStoreInst asi = new ArrayStoreInst(nai, module.constants().getConstant(i), toStore);
				insts.add(asi);
			}
			return new Pair<Value, List<Instruction>>(nai, insts.build());
		}

		private void mapIOInfo() {
			ImmutableMap.Builder<Worker<?, ?>, ImmutableSortedSet<IOInfo>> inputIOs = ImmutableMap.builder(),
					outputIOs = ImmutableMap.builder();
			for (Worker<?, ?> w : workers) {
				ImmutableSortedSet.Builder<IOInfo> inputs = ImmutableSortedSet.orderedBy(IOInfo.TOKEN_SORT),
						outputs = ImmutableSortedSet.orderedBy(IOInfo.TOKEN_SORT);
				for (IOInfo info : ioinfo)
					if (w.equals(info.downstream()))
						inputs.add(info);
					else if (w.equals(info.upstream()))
						outputs.add(info);
				inputIOs.put(w, inputs.build());
				outputIOs.put(w, outputs.build());
			}
			this.inputIOs = inputIOs.build();
			this.outputIOs = outputIOs.build();
		}
	}

	private final class BufferData {
		/**
		 * The field (for intracore buffers) or two fields (for intercore
		 * buffers) pointing to the buffers.  If there are two fields, the first
		 * is the reader's buffer (which gets filled with initial buffering) and
		 * the second is the writer's buffer (which is empty).
		 */
		public final ImmutableList<Field> fields;
		/**
		 * The buffer capacity.
		 */
		public final int capacity;
		/**
		 * The buffer initial size.  This is generally less than the capacity
		 * for intracore buffers introduced by peeking.  Intercore buffers
		 * always get filled to capacity.
		 */
		public final int initialSize;
		/**
		 * Note that either upstream xor downstream may be null.
		 */
		private BufferData(Worker<?, ?> upstream, Worker<?, ?> downstream) {
			final String upstreamId = upstream != null ? Integer.toString(Workers.getIdentifier(upstream)) : "input";
			final String downstreamId = downstream != null ? Integer.toString(Workers.getIdentifier(downstream)) : "output";
			final StreamNode upstreamNode = streamNodes.get(upstream);
			final StreamNode downstreamNode = streamNodes.get(downstream);
			RegularType objArrayTy = module.types().getRegularType(Object[].class);

			String fieldName = "buf_"+upstreamId+"_"+downstreamId;
			//Do we need to synchronize to pass data between these two nodes?
			boolean intercore = upstreamNode != null && downstreamNode != null && upstreamNode != downstreamNode;
			if (intercore)
				fields = ImmutableList.of(new Field(objArrayTy, fieldName+"a", EnumSet.of(Modifier.PRIVATE, Modifier.STATIC), blobKlass),
					new Field(objArrayTy, fieldName+"b", EnumSet.of(Modifier.PRIVATE, Modifier.STATIC), blobKlass));
			else
				fields = ImmutableList.of(new Field(objArrayTy, fieldName, EnumSet.of(Modifier.PRIVATE, Modifier.STATIC), blobKlass));

			if (downstream != null) {
				//If upstream is null, it's the global input, channel 0.
				int chanIdx = upstream != null ? Workers.getPredecessors(downstream).indexOf(upstream) : 0;
				assert chanIdx != -1;
				int pop = downstream.getPopRates().get(chanIdx).max(), peek = downstream.getPeekRates().get(chanIdx).max();
				int excessPeeks = Math.max(peek - pop, 0);
				this.capacity = downstreamNode.execsPerNodeExec.get(downstream) * schedule.get(downstreamNode) * multiplier * pop + excessPeeks;
				this.initialSize = intercore ? excessPeeks : capacity;
			} else { //downstream == null
				//If downstream is null, it's the global output, channel 0.
				int push = upstream.getPushRates().get(0).max();
				this.capacity = upstreamNode.execsPerNodeExec.get(upstream) * schedule.get(upstreamNode) * multiplier * push;
				this.initialSize = 0;
			}
		}
	}

	public static void main(String[] args) {
		OneToOneElement<Integer, Integer> graph = new Splitjoin<>(new RoundrobinSplitter<Integer>(), new RoundrobinJoiner<Integer>(), new Identity<Integer>(), new Identity<Integer>());
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor(new ChannelFactory() {
			@Override
			public <E> Channel<E> makeChannel(Worker<?, E> upstream, Worker<E, ?> downstream) {
				return new EmptyChannel<>();
			}
		});
		graph.visit(cwv);
		Set<Worker<?, ?>> workers = Workers.getAllWorkersInGraph(cwv.getSource());
		Configuration config = new CompilerBlobFactory().getDefaultConfiguration(workers);
		int maxNumCores = 1;
		Compiler compiler = new Compiler(workers, config, maxNumCores);
		Blob blob = compiler.compile();
		blob.getCoreCount();
	}
}
