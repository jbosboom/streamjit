/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.impl.compiler;

import edu.mit.streamjit.util.bytecode.Klass;
import edu.mit.streamjit.util.bytecode.LocalVariable;
import edu.mit.streamjit.util.bytecode.Value;
import edu.mit.streamjit.util.bytecode.BasicBlock;
import edu.mit.streamjit.util.bytecode.Cloning;
import edu.mit.streamjit.util.bytecode.Module;
import edu.mit.streamjit.util.bytecode.Argument;
import edu.mit.streamjit.util.bytecode.ModuleClassLoader;
import edu.mit.streamjit.util.bytecode.Field;
import edu.mit.streamjit.util.bytecode.Modifier;
import edu.mit.streamjit.util.bytecode.Method;
import static com.google.common.base.Preconditions.*;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.math.IntMath;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Joiner;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.Splitter;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.StreamCompilationFailedException;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import edu.mit.streamjit.impl.blob.DrainData;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.SwitchParameter;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.IOInfo;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.util.bytecode.insts.ArrayLoadInst;
import edu.mit.streamjit.util.bytecode.insts.ArrayStoreInst;
import edu.mit.streamjit.util.bytecode.insts.BinaryInst;
import edu.mit.streamjit.util.bytecode.insts.BranchInst;
import edu.mit.streamjit.util.bytecode.insts.CallInst;
import edu.mit.streamjit.util.bytecode.insts.CastInst;
import edu.mit.streamjit.util.bytecode.insts.Instruction;
import edu.mit.streamjit.util.bytecode.insts.JumpInst;
import edu.mit.streamjit.util.bytecode.insts.LoadInst;
import edu.mit.streamjit.util.bytecode.insts.NewArrayInst;
import edu.mit.streamjit.util.bytecode.insts.PhiInst;
import edu.mit.streamjit.util.bytecode.insts.ReturnInst;
import edu.mit.streamjit.util.bytecode.insts.StoreInst;
import edu.mit.streamjit.util.bytecode.types.ArrayType;
import edu.mit.streamjit.util.bytecode.types.FieldType;
import edu.mit.streamjit.util.bytecode.types.MethodType;
import edu.mit.streamjit.util.bytecode.types.RegularType;
import edu.mit.streamjit.impl.interp.ArrayChannel;
import edu.mit.streamjit.impl.interp.Channel;
import edu.mit.streamjit.impl.interp.ChannelFactory;
import edu.mit.streamjit.impl.interp.Interpreter;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;
import edu.mit.streamjit.test.apps.fmradio.FMRadio;
import edu.mit.streamjit.util.Pair;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleProxies;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.SwitchPoint;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
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
	private final DrainData initialState;
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
	private Schedule<StreamNode> schedule;
	private Schedule<Worker<?, ?>> initSchedule;
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
	/**
	 * Contains static fields so that final static fields in blobKlass can load
	 * from them in the blobKlass static initializer.
	 */
	private final Klass fieldHelperKlass;
	public Compiler(Set<Worker<?, ?>> workers, Configuration config, int maxNumCores, DrainData initialState) {
		this.workers = workers;
		this.config = config;
		this.maxNumCores = maxNumCores;
		this.initialState = initialState;
		this.ioinfo = IOInfo.externalEdges(workers);

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
				Collections.<Klass>emptyList(),
				module);
		blobKlass.modifiers().addAll(EnumSet.of(Modifier.PUBLIC, Modifier.FINAL));
		this.fieldHelperKlass = new Klass(packagePrefix + "FieldHelper",
				module.getKlass(Object.class),
				Collections.<Klass>emptyList(),
				module);
		fieldHelperKlass.modifiers().addAll(EnumSet.of(Modifier.PUBLIC, Modifier.FINAL));
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
		computeInitSchedule();
		declareBuffers();
		//We generate a work method for each worker (which may result in
		//duplicates, but is required in general to handle worker fields), then
		//generate core code that stitches them together and does any
		//required data movement.
		for (Worker<?, ?> w : streamNodes.keySet())
			makeWorkMethod(w);
		for (StreamNode n : ImmutableSet.copyOf(streamNodes.values()))
			n.makeWorkMethod();
		generateCoreCode();
		generateStaticInit();
		addBlobPlumbing();

		Path path = (Path)config.getExtraData("dumpFile");
		if (path != null) {
			try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
				module.dump(writer);
			} catch (IOException ex) {
				//Don't fail just because we couldn't dump, but do complain.
				ex.printStackTrace();
			}
		}

		return instantiateBlob();
	}

	/**
	 * Fuses StreamNodes as directed by the configuration.
	 */
	private void fuse() {
		//TODO: check this works/doesn't work with peeking or state.
		Set<Integer> eligible = new HashSet<>();
		for (StreamNode n : streamNodes.values()) {
			SwitchParameter<Boolean> parameter = config.getParameter("fuse"+n.id, SwitchParameter.class, Boolean.class);
			if (!n.isPeeking() && parameter.getValue())
				eligible.add(n.id);
		}

		boolean fused;
		do {
			fused = false;
			ImmutableSortedSet<StreamNode> nodes = ImmutableSortedSet.copyOf(streamNodes.values());
			for (StreamNode n : nodes) {
				Set<StreamNode> preds = n.predecessorNodes();
				if (eligible.contains(n.id) && preds.size() == 1) {
					new StreamNode(preds.iterator().next(), n); //adds itself to maps
					fused = true;
				}
			}
		} while (fused);
	}

	/**
	 * Computes buffer capacity and initial sizes, declaring (but not
	 * arranging for initialization of) the blob class fields pointing to the
	 * buffers.
	 */
	private void declareBuffers() {
		ImmutableMap.Builder<Token, BufferData> builder = ImmutableMap.<Token, BufferData>builder();
		for (IOInfo info : IOInfo.internalEdges(workers))
			//Only declare buffers for worker pairs not in the same node.  If
			//a node needs internal buffering, it handles that itself.  (This
			//implies that peeking filters cannot be fused upwards, but that's
			//a bad idea anyway.)
			if (!streamNodes.get(info.upstream()).equals(streamNodes.get(info.downstream())))
				builder.put(info.token(), makeBuffers(info));
		//Make buffers for the inputs and outputs of this blob (which may or
		//may not be overall inputs of the stream graph).
		for (IOInfo info : ioinfo)
			if (firstWorker.equals(info.downstream()) || lastWorker.equals(info.upstream()))
				builder.put(info.token(), makeBuffers(info));
		buffers = builder.build();
	}

	/**
	 * Creates buffers in the blobKlass for the given workers, returning a
	 * BufferData describing the buffers created.
	 *
	 * One of upstream xor downstream may be null for the overall input and
	 * output.
	 */
	private BufferData makeBuffers(IOInfo info) {
		Worker<?, ?> upstream = info.upstream(), downstream = info.downstream();
		Token token = info.token();
		assert upstream != null || downstream != null;

		final String upstreamId = upstream != null ? Integer.toString(token.getUpstreamIdentifier()) : "input";
		final String downstreamId = downstream != null ? Integer.toString(token.getDownstreamIdentifier()) : "output";
		final StreamNode upstreamNode = streamNodes.get(upstream);
		final StreamNode downstreamNode = streamNodes.get(downstream);
		RegularType objArrayTy = module.types().getRegularType(Object[].class);

		String fieldName = "buf_"+upstreamId+"_"+downstreamId;
		assert downstreamNode != upstreamNode;
		String readerBufferFieldName = token.isOverallOutput() ? null : fieldName + "r";
		String writerBufferFieldName = token.isOverallInput() ? null : fieldName + "w";
		for (String field : new String[]{readerBufferFieldName, writerBufferFieldName})
			if (field != null)
				new Field(objArrayTy, field, EnumSet.of(Modifier.PUBLIC, Modifier.STATIC), blobKlass);

		int capacity, initialSize, unconsumedItems;
		if (info.isInternal()) {
			assert upstreamNode != null && downstreamNode != null;
			assert !upstreamNode.equals(downstreamNode) : "shouldn't be buffering on intra-node edge";
			capacity = initialSize = getInitBufferDelta(info);
			unconsumedItems = capacity - getThroughput(info);
			assert unconsumedItems >= 0 : unconsumedItems;
		} else if (info.isInput()) {
			assert downstream != null;
			int chanIdx = info.getDownstreamChannelIndex();
			int pop = downstream.getPopRates().get(chanIdx).max(), peek = downstream.getPeekRates().get(chanIdx).max();
			unconsumedItems = Math.max(peek - pop, 0);
			capacity = initialSize = downstreamNode.internalSchedule.getExecutions(downstream) * schedule.getExecutions(downstreamNode)  * pop + unconsumedItems;
		} else if (info.isOutput()) {
			int push = upstream.getPushRates().get(info.getUpstreamChannelIndex()).max();
			capacity = upstreamNode.internalSchedule.getExecutions(upstream) * schedule.getExecutions(upstreamNode)  * push;
			initialSize = 0;
			unconsumedItems = 0;
		} else
			throw new AssertionError(info);

		return new BufferData(token, readerBufferFieldName, writerBufferFieldName, capacity, initialSize, unconsumedItems);
	}

	/**
	 * Computes the initialization schedule using the scheduler.
	 */
	private void computeInitSchedule() {
		Schedule.Builder<Worker<?, ?>> builder = Schedule.builder();
		builder.addAll(workers);
		for (IOInfo info : IOInfo.internalEdges(workers)) {
			Schedule.Builder<Worker<?, ?>>.BufferingConstraintBuilder constraint =
					builder.connect(info.upstream(), info.downstream())
					.push(info.upstream().getPushRates().get(info.getUpstreamChannelIndex()).max())
					.pop(info.downstream().getPopRates().get(info.getDownstreamChannelIndex()).max())
					.peek(info.downstream().getPeekRates().get(info.getDownstreamChannelIndex()).max());

			int initialBufferSize = 0;
			if (initialState != null) {
				ImmutableList<Object> data = initialState.getData(info.token());
				if (data != null)
					initialBufferSize = data.size();
			}

			//Inter-node edges require at least a steady-state's worth of
			//buffering (to avoid synchronization); intra-node edges cannot have
			//any buffering at all.
			StreamNode upstreamNode = streamNodes.get(info.upstream());
			StreamNode downstreamNode = streamNodes.get(info.downstream());
			if (!upstreamNode.equals(downstreamNode))
				constraint.bufferAtLeast(getSteadyStateBufferSize(info) - initialBufferSize);
			else
				constraint.bufferExactly(-initialBufferSize);
		}

		try {
			initSchedule = builder.build();
		} catch (Schedule.ScheduleException ex) {
			throw new StreamCompilationFailedException("couldn't find initialization schedule", ex);
		}
	}

	private int getInitBufferDelta(IOInfo info) {
		int pushRate = info.upstream().getPushRates().get(info.getUpstreamChannelIndex()).max();
		int popRate = info.downstream().getPopRates().get(info.getDownstreamChannelIndex()).max();
		return initSchedule.getExecutions(info.upstream()) * pushRate
				- initSchedule.getExecutions(info.downstream()) * popRate;
	}

	private int getThroughput(IOInfo info) {
		int pushRate = info.upstream().getPushRates().get(info.getUpstreamChannelIndex()).max();
		StreamNode node = streamNodes.get(info.upstream());
		return schedule.getExecutions(node) * node.internalSchedule.getExecutions(info.upstream()) * pushRate;
	}

	private int getSteadyStateBufferSize(IOInfo info) {
		//TODO: factor out excessPeeks computation into its own function
		int excessPeeks = Math.max(0, info.downstream().getPeekRates().get(info.getDownstreamChannelIndex()).max() - info.downstream().getPopRates().get(info.getDownstreamChannelIndex()).max());
		return getThroughput(info) + excessPeeks;
	}

	/**
	 * Make the work method for the given worker.  We actually make two methods
	 * here: first we make a copy with a dummy receiver argument, just to have a
	 * copy to work with.  After remapping every use of that receiver (remapping
	 * field accesses to the worker's static fields, remapping JIT-hooks to
	 * their implementations, and remapping utility methods in the worker class
	 * recursively), we then externalEdges the actual work method without the receiver
	 * argument.
	 * @param worker
	 */
	private void makeWorkMethod(Worker<?, ?> worker) {
		StreamNode node = streamNodes.get(worker);
		int id = Workers.getIdentifier(worker);
		int numInputs = getNumInputs(worker);
		int numOutputs = getNumOutputs(worker);
		Klass workerKlass = module.getKlass(worker.getClass());
		Method oldWork = workerKlass.getMethodByVirtual("work", module.types().getMethodType(void.class, worker.getClass()));
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
		//Actually, we don't!  We need the updates to carry over to further
		//iterations within the nodework.  My thinking was that we could
		//precompute these to avoid repeated allocations, or something.
//		Method clone = Iterables.getOnlyElement(module.getKlass(Object.class).getMethods("clone"));
//		CallInst ioffsetCloneCall = new CallInst(clone, newWork.arguments().get(2));
//		entryBlock.instructions().add(ioffsetCloneCall);
//		CastInst ioffsetCast = new CastInst(module.types().getArrayType(int[].class), ioffsetCloneCall);
//		entryBlock.instructions().add(ioffsetCast);
		Argument ioffsetCast = newWork.arguments().get(2);
		LocalVariable ioffsetCopy = new LocalVariable((RegularType)ioffsetCast.getType(), "ioffsetCopy", newWork);
		StoreInst popCountInit = new StoreInst(ioffsetCopy, ioffsetCast);
		popCountInit.setName("ioffsetInit");
		entryBlock.instructions().add(popCountInit);

//		CallInst ooffsetCloneCall = new CallInst(clone, newWork.arguments().get(5));
//		entryBlock.instructions().add(ooffsetCloneCall);
//		CastInst ooffsetCast = new CastInst(module.types().getArrayType(int[].class), ooffsetCloneCall);
//		entryBlock.instructions().add(ooffsetCast);
		Argument ooffsetCast = newWork.arguments().get(5);
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
			Method peek1Filter = filterKlass.getMethod("peek", module.types().getMethodType(Object.class, Filter.class, int.class));
			assert peek1Filter != null;
			Method peek1Splitter = splitterKlass.getMethod("peek", module.types().getMethodType(Object.class, Splitter.class, int.class));
			assert peek1Splitter != null;
			Method pop1Filter = filterKlass.getMethod("pop", module.types().getMethodType(Object.class, Filter.class));
			assert pop1Filter != null;
			Method pop1Splitter = splitterKlass.getMethod("pop", module.types().getMethodType(Object.class, Splitter.class));
			assert pop1Splitter != null;
			Method push1Filter = filterKlass.getMethod("push", module.types().getMethodType(void.class, Filter.class, Object.class));
			assert push1Filter != null;
			Method push1Joiner = joinerKlass.getMethod("push", module.types().getMethodType(void.class, Joiner.class, Object.class));
			assert push1Joiner != null;
			Method peek2 = joinerKlass.getMethod("peek", module.types().getMethodType(Object.class, Joiner.class, int.class, int.class));
			assert peek2 != null;
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

			if (method.equals(peek1Filter) || method.equals(peek1Splitter) || method.equals(peek2)) {
				Value channelNumber = method.equals(peek2) ? ci.getArgument(1) : module.constants().getSmallestIntConstant(0);
				Argument ichannels = rwork.getArgument("ichannels");
				ArrayLoadInst channel = new ArrayLoadInst(ichannels, channelNumber);
				LoadInst ioffsets = new LoadInst(rwork.getLocalVariable("ioffsetCopy"));
				ArrayLoadInst offsetBase = new ArrayLoadInst(ioffsets, channelNumber);
				Value peekIndex = method.equals(peek2) ? ci.getArgument(2) : ci.getArgument(1);
				BinaryInst offset = new BinaryInst(offsetBase, BinaryInst.Operation.ADD, peekIndex);
				ArrayLoadInst item = new ArrayLoadInst(channel, offset);
				item.setName("peekedItem");
				inst.replaceInstWithInsts(item, channel, ioffsets, offsetBase, offset, item);
			} else if (method.equals(pop1Filter) || method.equals(pop1Splitter) || method.equals(pop2)) {
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
		} else if (inst instanceof StoreInst) {
			StoreInst si = (StoreInst)inst;
			assert si.getLocation() instanceof Field;
			StoreInst replacement = new StoreInst(streamNodes.get(worker).fields.get(worker, (Field)si.getLocation()), si.getData());
			si.replaceInstWithInst(replacement);
		} else
			throw new AssertionError("Couldn't eliminate reciever: "+inst);
	}

	private int getNumInputs(Worker<?, ?> w) {
		return Workers.getInputChannels(w).size();
	}

	private int getNumOutputs(Worker<?, ?> w) {
		return Workers.getOutputChannels(w).size();
	}

	/**
	 * Generates the corework* methods, which contain the steady-state code for
	 * each core.  (The buffers are assumed to be already prepared.)
	 */
	private void generateCoreCode() {
		List<StreamNode> nodes = new ArrayList<>(ImmutableSet.copyOf(streamNodes.values()));
		Collections.sort(nodes, new Comparator<StreamNode>() {
			@Override
			public int compare(StreamNode o1, StreamNode o2) {
				return Integer.compare(o1.id, o2.id);
			}
		});

		//For each core, a list of the nodework-iteration pairs allocated to
		//that core.
		List<List<Pair<Method, Range<Integer>>>> allocations = new ArrayList<>(maxNumCores);
		for (int i = 0; i < maxNumCores; ++i)
			allocations.add(new ArrayList<Pair<Method, Range<Integer>>>());

		for (StreamNode node : nodes) {
			int iterations = schedule.getExecutions(node);
			int i = 0;
			for (int core = 0; core < allocations.size() && i < iterations; ++core) {
				String name = String.format("node%dcore%diter", node.id, core);
				IntParameter parameter = config.getParameter(name, IntParameter.class);
				if (parameter == null || parameter.getValue() == 0) continue;

				//If the node is stateful, we must put all allocations on the
				//same core. Arbitrarily pick the first core with an allocation.
				//If no cores have an allocation,
				int allocation = node.isStateful() ? iterations :
						Math.min(parameter.getValue(), iterations - i);
				allocations.get(core).add(new Pair<>(node.workMethod, Range.closedOpen(i, i+allocation)));
				i += allocation;
			}

			//If we have iterations left over not assigned to a core,
			//arbitrarily put them on core 0.
			if (i < iterations)
				allocations.get(0).add(new Pair<>(node.workMethod, Range.closedOpen(i, iterations)));
		}

		//For each core with allocations, make a corework method.
		for (int core = 0; core < allocations.size(); ++core) {
			List<Pair<Method, Range<Integer>>> stuff = allocations.get(core);
			if (stuff.isEmpty()) continue;

			Method coreCode = new Method("corework"+core, module.types().getMethodType(void.class), EnumSet.of(Modifier.PUBLIC, Modifier.STATIC), blobKlass);
			BasicBlock previousBlock = new BasicBlock(module, "entry");
			coreCode.basicBlocks().add(previousBlock);
			for (Pair<Method, Range<Integer>> allocation : stuff) {
				BasicBlock loop = makeCallLoop(allocation.first, allocation.second.lowerEndpoint(), allocation.second.upperEndpoint(), previousBlock, allocation.first.getName());
				coreCode.basicBlocks().add(loop);
				if (previousBlock.getTerminator() == null)
					previousBlock.instructions().add(new JumpInst(loop));
				else
					((BranchInst)previousBlock.getTerminator()).setOperand(3, loop);
				previousBlock = loop;
			}

			BasicBlock exit = new BasicBlock(module, "exit");
			exit.instructions().add(new ReturnInst(module.types().getVoidType()));
			coreCode.basicBlocks().add(exit);
			//We only generate corework for cores with allocations, so we should
			//always have an allocation above us.
			assert previousBlock.getTerminator() instanceof BranchInst : previousBlock.getTerminator();
			((BranchInst)previousBlock.getTerminator()).setOperand(3, exit);
		}
	}

	/**
	 * Creates a block that calls the given method with arguments from begin to
	 * end.  Block ends in a BranchInst with its false branch not set (to be set
	 * to the next block by the caller).
	 */
	private BasicBlock makeCallLoop(Method method, int begin, int end, BasicBlock previousBlock, String loopName) {
		int tripcount = end-begin;
		assert tripcount > 0 : String.format("0 tripcount in makeCallLoop: %s, %d, %d, %s, %s", method, begin, end, previousBlock, loopName);

		BasicBlock body = new BasicBlock(module, loopName+"_loop");

		PhiInst count = new PhiInst(module.types().getRegularType(int.class));
		count.put(previousBlock, module.constants().getConstant(begin));
		body.instructions().add(count);

		CallInst call = new CallInst(method, count);
		body.instructions().add(call);

		BinaryInst increment = new BinaryInst(count, BinaryInst.Operation.ADD, module.constants().getConstant(1));
		body.instructions().add(increment);
		count.put(body, increment);

		BranchInst branch = new BranchInst(increment, BranchInst.Sense.LT, module.constants().getConstant(end), body, null);
		body.instructions().add(branch);
		return body;
	}

	private void generateStaticInit() {
		Method clinit = new Method("<clinit>",
				module.types().getMethodType(void.class),
				EnumSet.of(Modifier.STATIC),
				blobKlass);

		//Generate fields in field helper, then copy them over in clinit.
		BasicBlock fieldBlock = new BasicBlock(module, "copyFieldsFromHelper");
		clinit.basicBlocks().add(fieldBlock);
		for (StreamNode node : ImmutableSet.copyOf(streamNodes.values()))
			for (Field cell : node.fields.values()) {
				Field helper = new Field(cell.getType().getFieldType(), cell.getName(), EnumSet.of(Modifier.PUBLIC, Modifier.STATIC), fieldHelperKlass);
				LoadInst li = new LoadInst(helper);
				StoreInst si = new StoreInst(cell, li);
				fieldBlock.instructions().add(li);
				fieldBlock.instructions().add(si);
			}

		BasicBlock bufferBlock = new BasicBlock(module, "newBuffers");
		clinit.basicBlocks().add(bufferBlock);
		for (BufferData data : ImmutableSortedSet.copyOf(buffers.values()))
			for (String fieldName : new String[]{data.readerBufferFieldName, data.writerBufferFieldName})
				if (fieldName != null) {
					Field field = blobKlass.getField(fieldName);
					NewArrayInst nai = new NewArrayInst((ArrayType)field.getType().getFieldType(), module.constants().getConstant(data.capacity));
					StoreInst si = new StoreInst(field, nai);
					bufferBlock.instructions().add(nai);
					bufferBlock.instructions().add(si);
				}

		BasicBlock exitBlock = new BasicBlock(module, "exit");
		clinit.basicBlocks().add(exitBlock);
		exitBlock.instructions().add(new ReturnInst(module.types().getVoidType()));

		for (int i = 0; i < clinit.basicBlocks().size()-1; ++i)
				clinit.basicBlocks().get(i).instructions().add(new JumpInst(clinit.basicBlocks().get(i+1)));
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
			initFieldHelper(mcl.loadClass(fieldHelperKlass.getName()));
			Class<?> blobClass = mcl.loadClass(blobKlass.getName());
			List<FieldData> fieldData = makeFieldData(blobClass);
			return new CompilerBlobHost(workers, config, initialState, blobClass, ImmutableList.copyOf(buffers.values()), fieldData, initSchedule.getSchedule());
		} catch (ClassNotFoundException | NoSuchFieldException ex) {
			throw new AssertionError(ex);
		}
	}

	private void initFieldHelper(Class<?> fieldHelperClass) {
		for (StreamNode node : ImmutableSet.copyOf(streamNodes.values()))
			for (Table.Cell<Worker<?, ?>, Field, Object> cell : node.fieldValues.cellSet())
				try {
					fieldHelperClass.getField(node.fields.get(cell.getRowKey(), cell.getColumnKey()).getName()).set(null, cell.getValue());
				} catch (NoSuchFieldException | IllegalAccessException ex) {
					throw new AssertionError(ex);
				}
	}

	private List<FieldData> makeFieldData(Class<?> blobClass) throws NoSuchFieldException {
		ImmutableList.Builder<FieldData> list = ImmutableList.builder();
		for (StreamNode node : ImmutableSet.copyOf(streamNodes.values()))
			for (Table.Cell<Worker<?, ?>, Field, Field> cell : node.fields.cellSet())
				if (Sets.intersection(cell.getColumnKey().modifiers(), EnumSet.of(Modifier.STATIC, Modifier.FINAL)).isEmpty())
					list.add(new FieldData(Workers.getIdentifier(cell.getRowKey()),
							cell.getColumnKey().getBackingField(),
							blobClass.getDeclaredField(cell.getValue().getName())));
		return list.build();
	}

	private void externalSchedule() {
		ImmutableSet<StreamNode> nodes = ImmutableSet.copyOf(streamNodes.values());
		Schedule.Builder<StreamNode> scheduleBuilder = Schedule.builder();
		scheduleBuilder.addAll(nodes);
		for (StreamNode n : nodes)
			n.constrainExternalSchedule(scheduleBuilder);
		scheduleBuilder.multiply(multiplier);

		try {
			schedule = scheduleBuilder.build();
		} catch (Schedule.ScheduleException ex) {
			throw new StreamCompilationFailedException("couldn't find external schedule", ex);
		}
	}

	private final class StreamNode implements Comparable<StreamNode> {
		private final int id;
		private final ImmutableSet<? extends Worker<?, ?>> workers;
		private final ImmutableSortedSet<IOInfo> ioinfo;
		private ImmutableMap<Worker<?, ?>, ImmutableSortedSet<IOInfo>> inputIOs, outputIOs;
		/**
		 * The number of individual worker executions per steady-state execution
		 * of the StreamNode.
		 */
		private Schedule<Worker<?, ?>> internalSchedule;
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

		private StreamNode(Worker<?, ?> worker) {
			this.id = Workers.getIdentifier(worker);
			this.workers = ImmutableSet.of(worker);
			this.ioinfo = ImmutableSortedSet.copyOf(IOInfo.TOKEN_SORT, IOInfo.externalEdges(workers));
			buildWorkerData(worker);

			assert !streamNodes.containsKey(worker);
			streamNodes.put(worker, this);
		}

		/**
		 * Fuses two StreamNodes.  They should not yet have been scheduled or
		 * had work functions constructed.
		 */
		private StreamNode(StreamNode a, StreamNode b) {
			assert streamNodes.values().contains(a);
			assert streamNodes.values().contains(b);
			this.id = Math.min(a.id, b.id);
			this.workers = ImmutableSet.<Worker<?, ?>>builder().addAll(a.workers).addAll(b.workers).build();
			this.ioinfo = ImmutableSortedSet.copyOf(IOInfo.TOKEN_SORT, IOInfo.externalEdges(workers));
			this.fields.putAll(a.fields);
			this.fields.putAll(b.fields);
			this.fieldValues.putAll(a.fieldValues);
			this.fieldValues.putAll(b.fieldValues);

			for (Worker<?, ?> w : a.workers)
				streamNodes.put(w, this);
			for (Worker<?, ?> w : b.workers)
				streamNodes.put(w, this);
		}

		public boolean isStateful() {
			for (Worker<?, ?> w : workers)
				if (w instanceof StatefulFilter)
					return true;
			return false;
		}

		public boolean isPeeking() {
			for (Worker<?, ?> w : workers) {
				for (int i = 0; i < w.getPeekRates().size(); ++i)
					if (w.getPeekRates().get(i).max() == Rate.DYNAMIC ||
							w.getPeekRates().get(i).max() > w.getPopRates().get(i).max())
						return true;
			}
			return false;
		}

		public Set<StreamNode> predecessorNodes() {
			ImmutableSet.Builder<StreamNode> set = ImmutableSet.builder();
			for (IOInfo info : ioinfo) {
				if (!info.isInput() || info.token().isOverallInput() || !streamNodes.containsKey(info.upstream()))
					continue;
				set.add(streamNodes.get(info.upstream()));
			}
			return set.build();
		}

		/**
		 * Compute the steady-state multiplicities of each worker in this node
		 * for each execution of the node.
		 */
		public void internalSchedule() {
			Schedule.Builder<Worker<?, ?>> scheduleBuilder = Schedule.builder();
			scheduleBuilder.addAll(workers);
			for (IOInfo info : IOInfo.internalEdges(workers))
				scheduleBuilder.connect(info.upstream(), info.downstream())
						.push(info.upstream().getPushRates().get(info.getUpstreamChannelIndex()).max())
						.pop(info.downstream().getPopRates().get(info.getDownstreamChannelIndex()).max())
						.peek(info.downstream().getPeekRates().get(info.getDownstreamChannelIndex()).max())
						.bufferExactly(0);
			try {
				this.internalSchedule = scheduleBuilder.build();
			} catch (Schedule.ScheduleException ex) {
				throw new StreamCompilationFailedException("couldn't find internal schedule for node "+id, ex);
			}
		}

		/**
		 * Adds constraints for each output edge of this StreamNode, with rates
		 * corrected for the internal schedule for both nodes.  (Only output
		 * edges so that we don't get duplicate constraints.)
		 */
		public void constrainExternalSchedule(Schedule.Builder<StreamNode> scheduleBuilder) {
			for (IOInfo info : ioinfo) {
				if (!info.isOutput() || info.token().isOverallOutput() || !streamNodes.containsKey(info.downstream()))
					continue;
				StreamNode other = streamNodes.get(info.downstream());
				int upstreamAdjust = internalSchedule.getExecutions(info.upstream());
				int downstreamAdjust = other.internalSchedule.getExecutions(info.downstream());
				scheduleBuilder.connect(this, other)
						.push(info.upstream().getPushRates().get(info.getUpstreamChannelIndex()).max() * upstreamAdjust)
						.pop(info.downstream().getPopRates().get(info.getDownstreamChannelIndex()).max() * downstreamAdjust)
						.peek(info.downstream().getPeekRates().get(info.getDownstreamChannelIndex()).max() * downstreamAdjust)
						.bufferExactly(0);
			}
		}

		private void buildWorkerData(Worker<?, ?> worker) {
			Klass workerKlass = module.getKlass(worker.getClass());

			//Build the new fields.
			Klass splitter = module.getKlass(Splitter.class),
					joiner = module.getKlass(Joiner.class),
					filter = module.getKlass(Filter.class);
			for (Klass k = workerKlass; !k.equals(filter) && !k.equals(splitter) && !k.equals(joiner); k = k.getSuperclass()) {
				for (Field f : k.fields()) {
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

			BasicBlock previousBlock = entryBlock;
			Map<Token, Value> localBuffers = new HashMap<>();
			ImmutableList<? extends Worker<?, ?>> orderedWorkers = Workers.topologicalSort(workers);
			for (Worker<?, ?> w : orderedWorkers) {
				int wid = Workers.getIdentifier(w);
				BasicBlock bufferBlock = new BasicBlock(module, "buffer"+wid);
				workMethod.basicBlocks().add(bufferBlock);
				if (previousBlock.getTerminator() == null)
					previousBlock.instructions().add(new JumpInst(bufferBlock));
				else
					((BranchInst)previousBlock.getTerminator()).setOperand(3, bufferBlock);
				previousBlock = bufferBlock;

				//Input buffers
				List<? extends Worker<?, ?>> preds = Workers.getPredecessors(w);
				List<Value> ichannels;
				List<Value> ioffsets = new ArrayList<>();
				if (preds.isEmpty()) {
					ichannels = ImmutableList.<Value>of(getReaderBuffer(Token.createOverallInputToken(w)));
					int r = w.getPopRates().get(0).max() * internalSchedule.getExecutions(w);
					BinaryInst offset = new BinaryInst(multiple, BinaryInst.Operation.MUL, module.constants().getConstant(r));
					offset.setName("ioffset0");
					previousBlock.instructions().add(offset);
					ioffsets.add(offset);
				} else {
					ichannels = new ArrayList<>(preds.size());
					for (int chanIdx = 0; chanIdx < preds.size(); ++chanIdx) {
						Worker<?, ?> p = preds.get(chanIdx);
						Token t = new Token(p, w);
						if (workers.contains(p)) {
							assert !buffers.containsKey(t) : "BufferData created for internal buffer";
							Value localBuffer = localBuffers.get(new Token(p, w));
							assert localBuffer != null : "Local buffer needed before created";
							ichannels.add(localBuffer);
							ioffsets.add(module.constants().getConstant(0));
						} else {
							ichannels.add(getReaderBuffer(t));
							int r = w.getPopRates().get(chanIdx).max() * internalSchedule.getExecutions(w);
							BinaryInst offset = new BinaryInst(multiple, BinaryInst.Operation.MUL, module.constants().getConstant(r));
							offset.setName("ioffset"+chanIdx);
							previousBlock.instructions().add(offset);
							ioffsets.add(offset);
						}
					}
				}

				Pair<Value, List<Instruction>> ichannelArray = createChannelArray(ichannels);
				ichannelArray.first.setName("ichannels_"+wid);
				previousBlock.instructions().addAll(ichannelArray.second);
				Pair<Value, List<Instruction>> ioffsetArray = createIntArray(ioffsets);
				ioffsetArray.first.setName("ioffsets_"+wid);
				previousBlock.instructions().addAll(ioffsetArray.second);
				Pair<Value, List<Instruction>> iincrementArray = createIntArray(Collections.<Value>nCopies(ioffsets.size(), module.constants().getConstant(1)));
				iincrementArray.first.setName("iincrements_"+wid);
				previousBlock.instructions().addAll(iincrementArray.second);

				//Output buffers
				List<? extends Worker<?, ?>> succs = Workers.getSuccessors(w);
				List<Value> ochannels;
				List<Value> ooffsets = new ArrayList<>();
				if (succs.isEmpty()) {
					ochannels = ImmutableList.<Value>of(getWriterBuffer(Token.createOverallOutputToken(w)));
					int r = w.getPushRates().get(0).max() * internalSchedule.getExecutions(w);
					BinaryInst offset = new BinaryInst(multiple, BinaryInst.Operation.MUL, module.constants().getConstant(r));
					offset.setName("ooffset0");
					previousBlock.instructions().add(offset);
					ooffsets.add(offset);
				} else {
					ochannels = new ArrayList<>(preds.size());
					for (int chanIdx = 0; chanIdx < succs.size(); ++chanIdx) {
						Worker<?, ?> s = succs.get(chanIdx);
						Token t = new Token(w, s);
						if (workers.contains(s)) {
							assert !buffers.containsKey(t) : "BufferData created for internal buffer";
							Value localBuffer = localBuffers.get(t);
							if (localBuffer == null) {
								int bufferSize = w.getPushRates().get(chanIdx).max() * internalSchedule.getExecutions(w);
								localBuffer = new NewArrayInst(module.types().getArrayType(Object[].class), module.constants().getConstant(bufferSize));
								localBuffer.setName(String.format("localbuf_%d_%d", t.getUpstreamIdentifier(), t.getDownstreamIdentifier()));
								localBuffers.put(t, localBuffer);
								previousBlock.instructions().add((Instruction)localBuffer);
							}
							ochannels.add(localBuffer);
							ooffsets.add(module.constants().getConstant(0));
						} else {
							ochannels.add(getWriterBuffer(t));
							int r = w.getPushRates().get(chanIdx).max() * internalSchedule.getExecutions(w);
							BinaryInst offset0 = new BinaryInst(multiple, BinaryInst.Operation.MUL, module.constants().getConstant(r));
							//Leave room to copy the excess peeks in front when
							//it's time to flip.
							BinaryInst offset = new BinaryInst(offset0, BinaryInst.Operation.ADD, module.constants().getConstant(buffers.get(t).excessPeeks));
							offset.setName("ooffset"+chanIdx);
							previousBlock.instructions().add(offset0);
							previousBlock.instructions().add(offset);
							ooffsets.add(offset);
						}
					}
				}

				Pair<Value, List<Instruction>> ochannelArray = createChannelArray(ochannels);
				ochannelArray.first.setName("ochannels_"+wid);
				previousBlock.instructions().addAll(ochannelArray.second);
				Pair<Value, List<Instruction>> ooffsetArray = createIntArray(ooffsets);
				ooffsetArray.first.setName("ooffsets_"+wid);
				previousBlock.instructions().addAll(ooffsetArray.second);
				Pair<Value, List<Instruction>> oincrementArray = createIntArray(Collections.<Value>nCopies(ooffsets.size(), module.constants().getConstant(1)));
				oincrementArray.first.setName("oincrements_"+wid);
				previousBlock.instructions().addAll(oincrementArray.second);

				CallInst ci = new CallInst(workerWorkMethods.get(w), ichannelArray.first, ioffsetArray.first, iincrementArray.first, ochannelArray.first, ooffsetArray.first, oincrementArray.first);
				BasicBlock loop = makeCallLoop(ci, 0, internalSchedule.getExecutions(w), previousBlock, "work"+wid);
				workMethod.basicBlocks().add(loop);
				if (previousBlock.getTerminator() == null)
					previousBlock.instructions().add(new JumpInst(loop));
				else
					((BranchInst)previousBlock.getTerminator()).setOperand(3, loop);
				previousBlock = loop;
			}

			BasicBlock exitBlock = new BasicBlock(module, "exit");
			workMethod.basicBlocks().add(exitBlock);
			exitBlock.instructions().add(new ReturnInst(module.types().getVoidType()));
			if (previousBlock.getTerminator() == null)
				previousBlock.instructions().add(new JumpInst(exitBlock));
			else
				((BranchInst)previousBlock.getTerminator()).setOperand(3, exitBlock);
		}

		private BasicBlock makeCallLoop(CallInst call, int begin, int end, BasicBlock previousBlock, String loopName) {
			int tripcount = end-begin;
			assert tripcount > 0 : String.format("0 tripcount in makeCallLoop: %s, %d, %d, %s, %s", call, begin, end, previousBlock, loopName);

			BasicBlock body = new BasicBlock(module, loopName+"_loop");

			PhiInst count = new PhiInst(module.types().getRegularType(int.class));
			count.put(previousBlock, module.constants().getConstant(begin));
			body.instructions().add(count);

			body.instructions().add(call);

			BinaryInst increment = new BinaryInst(count, BinaryInst.Operation.ADD, module.constants().getConstant(1));
			body.instructions().add(increment);
			count.put(body, increment);

			BranchInst branch = new BranchInst(increment, BranchInst.Sense.LT, module.constants().getConstant(end), body, null);
			body.instructions().add(branch);
			return body;
		}

		private Field getReaderBuffer(Token t) {
			return blobKlass.getField(buffers.get(t).readerBufferFieldName);
		}
		private Field getWriterBuffer(Token t) {
			return blobKlass.getField(buffers.get(t).writerBufferFieldName);
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

		@Override
		public int compareTo(StreamNode other) {
			return Integer.compare(id, other.id);
		}
	}

	/**
	 * Holds information about buffers.  This class is used both during
	 * compilation and at runtime, so it doesn't directly refer to the Compiler
	 * or IR-level constructs, to ensure they can be garbage collected when
	 * compilation finishes.
	 */
	private static final class BufferData implements Comparable<BufferData> {
		/**
		 * The Token for the edge this buffer is on.
		 */
		public final Token token;
		/**
		 * The names of the reader and writer buffers.  The reader buffer is the
		 * one initially filled with data items for peeking purposes.
		 *
		 * The overall input buffer has no writer buffer; the overall output
		 * buffer has no reader buffer.
		 */
		public final String readerBufferFieldName, writerBufferFieldName;
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
		 * The number of items peeked at but not popped; that is, the number of
		 * unconsumed items in the reader buffer that must be copied to the
		 * front of the writer buffer when flipping buffers.
		 */
		public final int excessPeeks;
		private BufferData(Token token, String readerBufferFieldName, String writerBufferFieldName, int capacity, int initialSize, int excessPeeks) {
			this.token = token;
			this.readerBufferFieldName = readerBufferFieldName;
			this.writerBufferFieldName = writerBufferFieldName;
			this.capacity = capacity;
			this.initialSize = initialSize;
			this.excessPeeks = excessPeeks;
			assert readerBufferFieldName != null || token.isOverallOutput() : this;
			assert writerBufferFieldName != null || token.isOverallInput() : this;
			assert capacity >= 0 : this;
			assert initialSize >= 0 && initialSize <= capacity : this;
			assert excessPeeks >= 0 && excessPeeks <= capacity : this;
		}

		@Override
		public int compareTo(BufferData other) {
			return token.compareTo(other.token);
		}

		@Override
		public String toString() {
			return String.format("[%s: r: %s, w: %s, init: %d, max: %d, peeks: %d]",
					token, readerBufferFieldName, writerBufferFieldName,
					initialSize, capacity, excessPeeks);
		}
	}

	/**
	 * Holds information about worker state fields.  This isn't used during
	 * compilation, so it refers to live java.lang.reflect.Fields constructed
	 * after classloading.
	 */
	private static final class FieldData {
		public final int id;
		public final java.lang.reflect.Field workerField, blobKlassField;
		private FieldData(int id, java.lang.reflect.Field workerField, java.lang.reflect.Field blobKlassField) {
			this.id = id;
			this.workerField = workerField;
			this.workerField.setAccessible(true);
			this.blobKlassField = blobKlassField;
			this.blobKlassField.setAccessible(true);
		}
	}

	private static final class CompilerBlobHost implements Blob {
		private final ImmutableSet<Worker<?, ?>> workers;
		private final Configuration configuration;
		private final DrainData initialState;
		private final ImmutableMap<Token, BufferData> bufferData;
		private final ImmutableSetMultimap<Integer, FieldData> fieldData;
		private final ImmutableMap<Worker<?, ?>, Integer> initSchedule;
		private final ImmutableMap<Token, Integer> initScheduleReqs;
		private final ImmutableSortedSet<Token> inputTokens, outputTokens, internalTokens;
		private final ImmutableMap<Token, Integer> minimumBufferSize;
		private final Class<?> blobClass;
		private final ImmutableMap<String, MethodHandle> blobClassMethods, blobClassFieldGetters, blobClassFieldSetters;
		private final Runnable[] runnables;
		private final SwitchPoint sp1 = new SwitchPoint(), sp2 = new SwitchPoint();
		private final CyclicBarrier barrier;
		private ImmutableMap<Token, Buffer> buffers;
		private ImmutableMap<Token, Channel<Object>> channelMap;
		private volatile Runnable drainCallback;
		private volatile DrainData drainData;

		public CompilerBlobHost(Set<Worker<?, ?>> workers, Configuration configuration, DrainData initialState, Class<?> blobClass, List<BufferData> bufferData, List<FieldData> fieldData, Map<Worker<?, ?>, Integer> initSchedule) {
			this.workers = ImmutableSet.copyOf(workers);
			this.configuration = configuration;
			this.initialState = initialState;
			this.initSchedule = ImmutableMap.copyOf(initSchedule);
			this.blobClass = blobClass;

			ImmutableMap.Builder<Token, BufferData> bufferDataBuilder = ImmutableMap.builder();
			for (BufferData d : bufferData)
				bufferDataBuilder.put(d.token, d);
			this.bufferData = bufferDataBuilder.build();

			ImmutableSetMultimap.Builder<Integer, FieldData> fieldDataBuilder = ImmutableSetMultimap.builder();
			for (FieldData d : fieldData)
				fieldDataBuilder.put(d.id, d);
			this.fieldData = fieldDataBuilder.build();

			ImmutableSet<IOInfo> ioinfo = IOInfo.externalEdges(workers);
			ImmutableMap.Builder<Token, Integer> initScheduleReqsBuilder = ImmutableMap.builder();
			for (IOInfo info : ioinfo) {
				if (!info.isInput()) continue;
				Worker<?, ?> worker = info.downstream();
				int index = info.getDownstreamChannelIndex();
				int popRate = worker.getPopRates().get(index).max();
				int peekRate = worker.getPeekRates().get(index).max();
				int excessPeeks = Math.max(0, peekRate - popRate);
				int required = popRate * initSchedule.get(worker) + this.bufferData.get(info.token()).initialSize;
				initScheduleReqsBuilder.put(info.token(), required);
			}
			this.initScheduleReqs = initScheduleReqsBuilder.build();

			ImmutableSortedSet.Builder<Token> inputTokensBuilder = ImmutableSortedSet.naturalOrder(), outputTokensBuilder = ImmutableSortedSet.naturalOrder();
			ImmutableMap.Builder<Token, Integer> minimumBufferSizeBuilder = ImmutableMap.builder();
			for (IOInfo info : ioinfo)
				if (info.isInput()) {
					inputTokensBuilder.add(info.token());
					BufferData data = this.bufferData.get(info.token());
					minimumBufferSizeBuilder.put(info.token(), Math.max(initScheduleReqs.get(info.token()), data.capacity));
				} else {
					outputTokensBuilder.add(info.token());
					//TODO: request enough for one or more steady-states worth of output?
					//We still have to support partial writes but we can give an
					//efficiency hint here.  Perhaps autotunable fraction?
					minimumBufferSizeBuilder.put(info.token(), 1);
				}
			this.inputTokens = inputTokensBuilder.build();
			this.outputTokens = outputTokensBuilder.build();
			this.minimumBufferSize = minimumBufferSizeBuilder.build();

			ImmutableSortedSet.Builder<Token> internalTokensBuilder = ImmutableSortedSet.naturalOrder();
			for (IOInfo info : IOInfo.internalEdges(workers))
				internalTokensBuilder.add(info.token());
			this.internalTokens = internalTokensBuilder.build();

			MethodHandles.Lookup lookup = MethodHandles.lookup();
			ImmutableMap.Builder<String, MethodHandle> methodBuilder = ImmutableMap.builder(),
					fieldGetterBuilder = ImmutableMap.builder(),
					fieldSetterBuilder = ImmutableMap.builder();
			try {
				java.lang.reflect.Method[] methods = blobClass.getDeclaredMethods();
				Arrays.sort(methods, new Comparator<java.lang.reflect.Method>() {
					@Override
					public int compare(java.lang.reflect.Method o1, java.lang.reflect.Method o2) {
						return o1.getName().compareTo(o2.getName());
					}
				});
				for (java.lang.reflect.Method m : methods) {
					m.setAccessible(true);
					methodBuilder.put(m.getName(), lookup.unreflect(m));
				}

				java.lang.reflect.Field[] fields = blobClass.getDeclaredFields();
				Arrays.sort(fields, new Comparator<java.lang.reflect.Field>() {
					@Override
					public int compare(java.lang.reflect.Field o1, java.lang.reflect.Field o2) {
						return o1.getName().compareTo(o2.getName());
					}
				});
				for (java.lang.reflect.Field f : fields) {
					f.setAccessible(true);
					fieldGetterBuilder.put(f.getName(), lookup.unreflectGetter(f));
					fieldSetterBuilder.put(f.getName(), lookup.unreflectSetter(f));
				}
			} catch (IllegalAccessException | SecurityException ex) {
				throw new AssertionError(ex);
			}
			this.blobClassMethods = methodBuilder.build();
			this.blobClassFieldGetters = fieldGetterBuilder.build();
			this.blobClassFieldSetters = fieldSetterBuilder.build();

			final java.lang.invoke.MethodType voidNoArgs = java.lang.invoke.MethodType.methodType(void.class);
			MethodHandle nop = MethodHandles.identity(Void.class).bindTo(null).asType(voidNoArgs);
			MethodHandle mainLoop, doInit, doAdjustBuffers, newAssertionError;
			try {
				mainLoop = lookup.findVirtual(CompilerBlobHost.class, "mainLoop", java.lang.invoke.MethodType.methodType(void.class, MethodHandle.class)).bindTo(this);
				doInit = lookup.findVirtual(CompilerBlobHost.class, "doInit", voidNoArgs).bindTo(this);
				doAdjustBuffers = lookup.findVirtual(CompilerBlobHost.class, "doAdjustBuffers", voidNoArgs).bindTo(this);
				newAssertionError = lookup.findConstructor(AssertionError.class, java.lang.invoke.MethodType.methodType(void.class, Object.class));
			} catch (IllegalAccessException | NoSuchMethodException ex) {
				throw new AssertionError(ex);
			}

			List<MethodHandle> coreWorkHandles = new ArrayList<>();
			for (Map.Entry<String, MethodHandle> methods : blobClassMethods.entrySet())
				if (methods.getKey().startsWith("corework"))
					coreWorkHandles.add(methods.getValue());

			this.runnables = new Runnable[coreWorkHandles.size()];
			for (int i = 0; i < runnables.length; ++i) {
				MethodHandle mainNop = mainLoop.bindTo(nop);
				MethodHandle mainCorework = mainLoop.bindTo(coreWorkHandles.get(i));
				MethodHandle overall = sp1.guardWithTest(mainNop, sp2.guardWithTest(mainCorework, nop));
				runnables[i] = MethodHandleProxies.asInterfaceInstance(Runnable.class, overall);
			}

			MethodHandle thrower = MethodHandles.throwException(void.class, AssertionError.class);
			MethodHandle doThrowAE = MethodHandles.filterReturnValue(newAssertionError.bindTo("Can't happen! Barrier action reached after draining?"), thrower);
			MethodHandle barrierAction = sp1.guardWithTest(doInit, sp2.guardWithTest(doAdjustBuffers, doThrowAE));
			this.barrier = new CyclicBarrier(runnables.length, MethodHandleProxies.asInterfaceInstance(Runnable.class, barrierAction));
		}

		@Override
		public Set<Worker<?, ?>> getWorkers() {
			return workers;
		}

		@Override
		public ImmutableSet<Token> getInputs() {
			return inputTokens;
		}

		@Override
		public ImmutableSet<Token> getOutputs() {
			return outputTokens;
		}

		@Override
		public int getMinimumBufferCapacity(Token token) {
			return minimumBufferSize.get(token);
		}

		@Override
		public void installBuffers(Map<Token, Buffer> buffers) {
			ImmutableMap.Builder<Token, Buffer> buffersBuilder = ImmutableMap.builder();
			for (Token t : getInputs())
				buffersBuilder.put(t, buffers.get(t));
			for (Token t : getOutputs())
				buffersBuilder.put(t, buffers.get(t));
			this.buffers = buffersBuilder.build();
		}

		@Override
		public int getCoreCount() {
			return runnables.length;
		}

		@Override
		public Runnable getCoreCode(int core) {
			return runnables[core];
		}

		@Override
		public void drain(Runnable callback) {
			drainCallback = callback;
		}

		@Override
		public DrainData getDrainData() {
			return drainData;
		}

		/**
		 * Rethrows all exceptions so that the thread dies.
		 */
		private void mainLoop(MethodHandle corework) throws Throwable {
			try {
				corework.invoke();
			} catch (Throwable ex) {
				//Deliberately break the barrier to release other threads.
				//Note that reset() does *not* break the barrier for threads not
				//already waiting at the barrier.
				Thread.currentThread().interrupt();
				try {
					barrier.await();
				} catch (InterruptedException expected) {}
				throw ex;
			}
			try {
				barrier.await();
			} catch (BrokenBarrierException ex) {
				throw ex;
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw ex;
			}
		}

		private List<Object> getInitialData(Token token) {
			if (initialState != null) {
				ImmutableList<Object> data = initialState.getData(token);
				if (data != null)
					return data;
			}
			return ImmutableList.of();
		}

		//TODO: to minimize memory usage during initialization, we should use
		//the interpreter to run a semi-pull schedule, or at least not allocate
		//channels until we need them.
		private void doInit() throws Throwable {
			//Create channels.
			ImmutableSet<IOInfo> allEdges = IOInfo.allEdges(workers);
			ImmutableMap.Builder<Token, Channel<Object>> channelMapBuilder = ImmutableMap.builder();
			for (IOInfo i : allEdges) {
				ArrayChannel<Object> c = new ArrayChannel<>();
				if (i.upstream() != null && !i.isInput()) {
					addOrSet(Workers.getOutputChannels(i.upstream()), i.getUpstreamChannelIndex(), c);
					c.ensureCapacity(initSchedule.get(i.upstream()) * i.upstream().getPushRates().get(i.getUpstreamChannelIndex()).max());
				}
				if (i.downstream() != null && !i.isOutput())
					addOrSet(Workers.getInputChannels(i.downstream()), i.getDownstreamChannelIndex(), c);

				for (Object o : getInitialData(i.token()))
					c.push(o);

				channelMapBuilder.put(i.token(), c);
			}
			this.channelMap = channelMapBuilder.build();

			//Fill input channels.
			for (IOInfo i : allEdges) {
				if (!i.isInput()) continue;
				int required = initScheduleReqs.get(i.token()) - getInitialData(i.token()).size();
				Channel<Object> channel = channelMap.get(i.token());
				Buffer buffer = buffers.get(i.token());
				Object[] data = new Object[required];
				//These are the first reads we do, so we can't "waste" our
				//interrupt here.
				while (!buffer.readAll(data))
					if (isDraining()) {
						doDrain(false, ImmutableList.<Token>of());
						return;
					}
				for (Object datum : data)
					channel.push(datum);
			}

			//Move state into worker fields.
			if (initialState != null) {
				for (Worker<?, ?> w : workers) {
					int id = Workers.getIdentifier(w);
					for (FieldData d : fieldData.get(id))
						d.workerField.set(w, initialState.getWorkerState(id, d.workerField.getName()));
				}
			}

			//Work workers in topological order.
			for (Worker<?, ?> worker : Workers.topologicalSort(workers)) {
				int iterations = initSchedule.get(worker);
				for (int i = 0; i < iterations; ++i)
					Workers.doWork(worker);
				//We can trim this worker's input channels.
				for (Worker<?, ?> p : Workers.getPredecessors(worker))
					((ArrayChannel<Object>)channelMap.get(new Token(p, worker))).trimToSize();
			}

			//Flush output (if any was generated?).
			for (Token output : getOutputs()) {
				Channel<Object> channel = channelMap.get(output);
				Buffer buffer = buffers.get(output);
				while (!channel.isEmpty()) {
					Object obj = channel.pop();
					while (!buffer.write(obj))
						/* deliberate empty statement */;
				}
			}

			//Move buffered items from channels to buffers.
			for (Map.Entry<Token, Channel<Object>> entry : channelMap.entrySet()) {
				Token token = entry.getKey();
				Channel<Object> channel = entry.getValue();
				BufferData data = bufferData.get(token);
				if (data == null) {
					assert channel.isEmpty() : "init schedule buffers within StreamNode";
					continue;
				}
				assert channel.size() == data.initialSize : String.format("%s: expected %d, got %d", token, data.initialSize, channel.size());
				if (data.readerBufferFieldName == null) {
					assert data.initialSize == 0;
					continue;
				}

				Object[] objs = Iterables.toArray(channel, Object.class);
				Object[] buffer = (Object[])blobClassFieldGetters.get(data.readerBufferFieldName).invokeExact();
				System.arraycopy(objs, 0, buffer, 0, objs.length);
				while (!channel.isEmpty())
					channel.pop();
			}

			for (Worker<?, ?> w : workers) {
				int id = Workers.getIdentifier(w);
				for (FieldData d : fieldData.get(id))
					d.blobKlassField.set(null, d.workerField.get(w));
			}

			SwitchPoint.invalidateAll(new SwitchPoint[]{sp1});
		}

		private void doAdjustBuffers() throws Throwable {
			//Flush output buffers.
			for (Token t : getOutputs()) {
				String fieldName = bufferData.get(t).writerBufferFieldName;
				Object[] data = (Object[])blobClassFieldGetters.get(fieldName).invokeExact();
				Buffer buffer = buffers.get(t);
				int written = 0;
				while (written < data.length)
					written += buffer.write(data, written, data.length - written);
			}

			//Copy unconsumed peek data, then flip buffers.
			for (BufferData data : bufferData.values()) {
				if (data.readerBufferFieldName == null || data.writerBufferFieldName == null) continue;
				Object[] reader = (Object[])blobClassFieldGetters.get(data.readerBufferFieldName).invokeExact();
				Object[] writer = (Object[])blobClassFieldGetters.get(data.writerBufferFieldName).invokeExact();
				if (data.excessPeeks > 0)
					System.arraycopy(reader, reader.length - data.excessPeeks, writer, 0, data.excessPeeks);
				blobClassFieldSetters.get(data.readerBufferFieldName).invokeExact(writer);
				blobClassFieldSetters.get(data.writerBufferFieldName).invokeExact(reader);
			}

			//Fill input buffers (draining-aware).
			ImmutableList<Token> inputList = getInputs().asList();
			for (int i = 0; i < inputList.size(); ++i) {
				Token t = inputList.get(i);
				BufferData data = bufferData.get(t);
				Object[] array = (Object[])blobClassFieldGetters.get(bufferData.get(t).readerBufferFieldName).invokeExact();
				System.arraycopy(array, array.length - data.excessPeeks, array, 0, data.excessPeeks);
				Buffer buffer = buffers.get(t);
				if (isDraining()) {
					//While draining, we can trust size() exactly, so we can
					//check before proceeding to avoid wasting the interrupt.
					if (buffer.size() >= array.length - data.excessPeeks) {
						boolean mustSucceed = buffer.readAll(array, data.excessPeeks);
						assert mustSucceed : "size() lies";
					} else {
						doDrain(true, inputList.subList(0, i));
						return;
					}
				} else
					while (!buffer.readAll(array, data.excessPeeks))
						if (isDraining()) {
							doDrain(true, inputList.subList(0, i));
							return;
						}
			}
		}

		/**
		 *
		 * @param nonInputReaderBuffersLive true iff non-input reader buffers
		 * are live (basically, true if we're draining from doAdjustBuffers,
		 * false if from doInit)
		 */
		private void doDrain(boolean nonInputReaderBuffersLive, List<Token> additionalLiveBuffers) throws Throwable {
			//We already have channels installed from initialization; we just
			//have to fill them if our internal buffers are live.  (If we're
			//draining during init some of the input channels are already
			//primed with data -- that's okay.)
			ImmutableSet.Builder<Token> live = ImmutableSet.builder();
			if (nonInputReaderBuffersLive)
				live.addAll(internalTokens);
			live.addAll(additionalLiveBuffers);
			for (Token t : live.build()) {
				BufferData data = bufferData.get(t);
				if (data != null) {
					Channel<Object> c = channelMap.get(t);
					assert c.isEmpty() : "data left in internal channel after init";
					Object[] buf = (Object[])blobClassFieldGetters.get(data.readerBufferFieldName).invokeExact();
					for (Object o : buf)
						c.push(o);
				}
			}

			for (Worker<?, ?> w : workers) {
				int id = Workers.getIdentifier(w);
				for (FieldData d : fieldData.get(id))
					d.workerField.set(w, d.blobKlassField.get(null));
			}

			//Create an interpreter and use it to drain stuff.
			//TODO: hack.  Make a proper Interpreter interface for this use case.
			//Obviously, we should make a DrainData for the interpreter!
			List<ChannelFactory> universe = Arrays.<ChannelFactory>asList(new ChannelFactory() {
				@Override
				@SuppressWarnings("unchecked")
				public <E> Channel<E> makeChannel(Worker<?, E> upstream, Worker<E, ?> downstream) {
					if (upstream == null)
						return (Channel<E>)channelMap.get(Token.createOverallInputToken(downstream));
					if (downstream == null)
						return (Channel<E>)channelMap.get(Token.createOverallOutputToken(upstream));
					return (Channel<E>)channelMap.get(new Token(upstream, downstream));
				}
				@Override
				public boolean equals(Object o) {
					return o != null && getClass() == o.getClass();
				}
				@Override
				public int hashCode() {
					return 10;
				}
			});
			Configuration config = Configuration.builder().addParameter(new Configuration.SwitchParameter<>("channelFactory", ChannelFactory.class, universe.get(0), universe)).build();
			Blob interp = new Interpreter.InterpreterBlobFactory().makeBlob(workers, config, 1, null /* TODO */);
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

			//TODO: null out blob class fields to permit GC.
		}

		private boolean isDraining() {
			return drainCallback != null;
		}

		private static void addOrSet(List list, int i, Object obj) {
			if (i < list.size())
				list.set(i, obj);
			else {
				assert i == 0;
				list.add(obj);
			}
		}
	}

//	public static void main(String[] args) throws Throwable {
//		OneToOneElement<Integer, Integer> graph = new Pipeline(new Pipeline(new edu.mit.streamjit.impl.common.TestFilters.Adder(20), new edu.mit.streamjit.impl.common.TestFilters.Batcher(2)), new Splitjoin(new edu.mit.streamjit.api.DuplicateSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new Pipeline(new Pipeline(new edu.mit.streamjit.impl.common.TestFilters.Multiplier(2), new edu.mit.streamjit.impl.common.TestFilters.Multiplier(2)), new Pipeline(new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new edu.mit.streamjit.impl.common.TestFilters.Batcher(10), new edu.mit.streamjit.api.Identity(), new edu.mit.streamjit.impl.common.TestFilters.Batcher(10)), new edu.mit.streamjit.impl.common.TestFilters.Batcher(2), new edu.mit.streamjit.impl.common.TestFilters.Batcher(10), new Splitjoin(new edu.mit.streamjit.api.RoundrobinSplitter(), new edu.mit.streamjit.api.RoundrobinJoiner(), new edu.mit.streamjit.impl.common.TestFilters.Adder(1), new edu.mit.streamjit.api.Identity(), new edu.mit.streamjit.impl.common.TestFilters.Batcher(2), new edu.mit.streamjit.impl.common.TestFilters.Batcher(10), new edu.mit.streamjit.impl.common.TestFilters.Batcher(2)), new Pipeline(new edu.mit.streamjit.impl.common.TestFilters.Batcher(2), new edu.mit.streamjit.api.Identity(), new edu.mit.streamjit.impl.common.TestFilters.Adder(1), new edu.mit.streamjit.impl.common.TestFilters.Batcher(2)))), new edu.mit.streamjit.impl.common.TestFilters.Adder(20), new Pipeline(new edu.mit.streamjit.impl.common.TestFilters.Multiplier(3)), new edu.mit.streamjit.impl.common.TestFilters.Batcher(2)));
//		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
//		graph.visit(cwv);
//		Set<Worker<?, ?>> workers = Workers.getAllWorkersInGraph(cwv.getSource());
//		Configuration config = new CompilerBlobFactory().getDefaultConfiguration(workers);
//		int maxNumCores = 1;
//		Compiler compiler = new Compiler(workers, config, maxNumCores, null);
//
//		Blob blob = compiler.compile();
//		Map<Token, Buffer> buffers = new HashMap<>();
//		for (Token t : blob.getInputs()) {
//			Buffer buf = Buffers.queueBuffer(new ArrayDeque<>(), Integer.MAX_VALUE);
//			for (int i = 0; i < 1000; ++i)
//				buf.write(i);
//			buffers.put(t, buf);
//		}
//		for (Token t : blob.getOutputs())
//			buffers.put(t, Buffers.queueBuffer(new ArrayDeque<>(), Integer.MAX_VALUE));
//		blob.installBuffers(buffers);
//
//		final AtomicBoolean drained = new AtomicBoolean();
//		blob.drain(new Runnable() {
//			@Override
//			public void run() {
//				drained.set(true);
//			}
//		});
//
//		Runnable r = blob.getCoreCode(0);
//		Buffer b = buffers.get(blob.getOutputs().iterator().next());
//		while (!drained.get()) {
//			r.run();
//			Object o;
//			while ((o = b.read()) != null)
//				System.out.println(o);
//		}
//		System.out.println(blob.getDrainData());
//	}

	public static void main(String[] args) {
		StreamCompiler sc = new CompilerStreamCompiler().multiplier(64).maxNumCores(8);
		Benchmark bm = new FMRadio.FMRadioBenchmarkProvider().iterator().next();
		Benchmarker.runBenchmark(bm, sc).get(0).print(System.out);
	}
}
