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
package edu.mit.streamjit.impl.common;

import edu.mit.streamjit.util.bytecode.MethodNodeBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import com.google.common.primitives.Ints;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.api.Portal;
import edu.mit.streamjit.api.IllegalStreamGraphException;
import edu.mit.streamjit.api.Rate;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import edu.mit.streamjit.impl.common.Workers.StreamPosition;
import edu.mit.streamjit.util.ReflectionUtils;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 1/29/2013
 */
public final class MessageConstraint {
	private final Portal<?> portal;
	private final Worker<?, ?> sender, recipient;
	private final int latency;
	private final StreamPosition direction;
	private final SDEPData sdepData;
	private MessageConstraint(Portal<?> portal, Worker<?, ?> sender, Worker<?, ?> recipient, int latency, StreamPosition direction, SDEPData sdepData) {
		this.portal = portal;
		this.sender = sender;
		this.recipient = recipient;
		this.latency = latency;
		this.direction = direction;
		this.sdepData = sdepData;
		if (this.direction == StreamPosition.UPSTREAM && this.latency < 0)
			throw new IllegalStreamGraphException("Impossible constraint: "+this.toString(), sender, recipient);
	}
	public Portal<?> getPortal() {
		return portal;
	}
	public Worker<?, ?> getSender() {
		return sender;
	}
	public Worker<?, ?> getRecipient() {
		return recipient;
	}
	public int getLatency() {
		return latency;
	}
	public StreamPosition getDirection() {
		return direction;
	}
	public long sdep(long downstreamExecutionCount) {
		return sdepData.sdep(downstreamExecutionCount);
	}
	public long reverseSdep(long upstreamExecutionCount) {
		return sdepData.reverseSdep(upstreamExecutionCount);
	}
	/**
	 * Computes the delivery time for a message given the current sender
	 * execution count.  Takes into account the message direction and latency.
	 *
	 * (The sender execution count must be provided as an argument, rather than
	 * retrieved via sender.getExecutions() directly, because we might not be
	 * executing in the interpreter.)
	 * @param senderExecutionCount the sender's execution count
	 * @return the execution of the recipient immediately before which the
	 * message should be delivered
	 */
	public long getDeliveryTime(long senderExecutionCount) {
		switch (getDirection()) {
			case DOWNSTREAM:
				//We add one to the reverseSdep result because we're
				//going downstream, thus e.g. if we're in our first
				//execution (sender.getExecutions() == 0), the message
				//should be delivered downstream at recipient's 0, but
				//we expect TTD to be greater than getExecutions().
				//Classic StreamIt adjusts the TTD at delivery to
				//account for this; we'll do it here.
				//TODO: is the inner +1 correct?
				return reverseSdep(senderExecutionCount + 1 + getLatency()) + 1;
			case UPSTREAM:
				//TODO: is the +1 correct?
				return sdep(senderExecutionCount + 1 + getLatency());
			case EQUAL:
			case INCOMPARABLE:
				throw new IllegalStreamGraphException("Illegal messaging: " + this, getSender(), getRecipient());
			default:
				throw new AssertionError();
		}
	}
	@Override
	public String toString() {
		return String.format("%s from %s to %s through %s after %d", direction, sender, recipient, portal, latency);
	}

	/**
	 * Grovels through the stream graph, discovering message constraints.
	 * @param graph
	 * @return
	 */
	public static List<MessageConstraint> findConstraints(Worker<?, ?> graph) {
		ImmutableList.Builder<MessageConstraint> mc = ImmutableList.builder();
		//Parsing bytecodes is (relatively) expensive; we only want to do it
		//once per class, no matter how many instances are in the stream graph.
		//If a class doesn't send messages, it maps to an empty list, and we do
		//nothing in the loop below.
		Map<Class<?>, ImmutableList<WorkerData>> workerDataCache = new HashMap<>();
		Map<Edge, SDEPData> sdepCache = new HashMap<>();

		for (Worker<?, ?> sender : Workers.getAllWorkersInGraph(graph)) {
			ImmutableList<WorkerData> datas = workerDataCache.get(sender.getClass());
			if (datas == null) {
				datas = buildWorkerData(sender);
				workerDataCache.put(sender.getClass(), datas);
			}

			for (WorkerData d : datas) {
				Portal<?> portal = d.getPortal(sender);
				int latency = d.getLatency(sender);
				for (Worker<?, ?> recipient : Portals.getRecipients(d.getPortal(sender))) {
					StreamPosition direction = Workers.compareStreamPosition(sender, recipient);
					Edge edge = direction == StreamPosition.UPSTREAM ? new Edge(sender, recipient) : new Edge(recipient, sender);
					SDEPData sdepData = computeSDEP(edge, sdepCache);
					//The message direction is opposite the relation between
					//sender and recipient.
					mc.add(new MessageConstraint(portal, sender, recipient, latency, direction.opposite(), sdepData));
				}
			}
		}

		return mc.build();
	}

	//<editor-fold defaultstate="collapsed" desc="WorkerData building (bytecode parsing)">
	/**
	 * WorkerData encapsulates the Field(s) and/or constant for the Portal and
	 * latency value of a particular class.  (Note that one class might have
	 * multiple WorkerDatas if it sends multiple messages.)  WorkerData also
	 * provides methods to easily get the field values from an object of the
	 * class.
	 */
	private static class WorkerData {
		private final Field portalField, latencyField;
		private final int constantLatency;
		WorkerData(Field portalField, Field latencyField) {
			this(portalField, latencyField, Integer.MIN_VALUE);
		}
		WorkerData(Field portalField, int constantLatency) {
			this(portalField, null, constantLatency);
		}
		WorkerData(Field portalField, Field latencyField, int constantLatency) {
			this.portalField = portalField;
			this.latencyField = latencyField;
			this.constantLatency = constantLatency;
			this.portalField.setAccessible(true);
			if (this.latencyField != null)
				this.latencyField.setAccessible(true);
		}
		public Portal<?> getPortal(Worker<?, ?> worker) {
			try {
				return (Portal<?>)portalField.get(worker);
			} catch (IllegalAccessException | IllegalArgumentException | NullPointerException | ExceptionInInitializerError ex) {
				throw new AssertionError("getting a portal object", ex);
			}
		}
		public int getLatency(Worker<?, ?> worker) {
			if (latencyField == null)
				return constantLatency;
			try {
				return latencyField.getInt(worker);
			} catch (IllegalAccessException | IllegalArgumentException | NullPointerException | ExceptionInInitializerError ex) {
				throw new AssertionError("getting latency from field", ex);
			}
		}
		@Override
		public String toString() {
			return portalField.toGenericString()+", "+(latencyField != null ? latencyField.toGenericString() : constantLatency);
		}
	}

	private static ImmutableList<WorkerData> buildWorkerData(Worker<?, ?> worker) {
		Class<?> klass = worker.getClass();
		//A worker can only send messages if it has a Portal field, and most
		//workers with Portal fields will send messages, so this is an efficient
		//and useful test to avoid the bytecode parse.
		if (!hasPortalField(worker.getClass()))
			return ImmutableList.of();
		return parseBytecodes(klass);
	}

	private static boolean hasPortalField(Class<?> klass) {
		for (Field f : ReflectionUtils.getAllFields(klass))
			if (f.getType().equals(Portal.class))
				return true;
		return false;
	}

	/**
	 * Parse the given class' bytecodes, looking for calls to getHandle() and
	 * returning WorkerDatas holding the calls' arguments.
	 * @param klass
	 * @return
	 */
	private static ImmutableList<WorkerData> parseBytecodes(Class<?> klass) {
		MethodNode mn;
		try {
			mn = MethodNodeBuilder.buildMethodNode(klass, "work", "()V");
		} catch (IOException ex) {
			throw new IllegalStreamGraphException("Couldn't get bytecode for "+klass.getName(), ex);
		} catch (NoSuchMethodException ex) {
			throw new AssertionError("Can't happen! Worker without work()? "+klass.getName(), ex);
		}

		ImmutableList.Builder<WorkerData> workerDatas = ImmutableList.builder();
		for (AbstractInsnNode insn = mn.instructions.getFirst(); insn != null; insn = insn.getNext()) {
			if (insn instanceof MethodInsnNode) {
				MethodInsnNode call = (MethodInsnNode)insn;
				if (call.name.equals("getHandle") && call.owner.equals(Type.getType(Portal.class).getInternalName()))
					workerDatas.add(dataFromCall(klass, call));
			}
		}

		return workerDatas.build();
	}

	/**
	 * Parse the given getHandle() call instruction and preceding instructions
	 * into a WorkerData.  This is a rather brittle pattern-matching job and
	 * will fail on obfuscated bytecodes.
	 * @param call
	 * @return
	 */
	private static WorkerData dataFromCall(Class<?> klass, MethodInsnNode call) {
		//Latency is either an integer constant or a getfield on this.
		Field latencyField = null;
		int constantLatency = Integer.MIN_VALUE;
		AbstractInsnNode latencyInsn = call.getPrevious();
		if (latencyInsn instanceof FieldInsnNode) {
			FieldInsnNode fieldInsn = (FieldInsnNode)latencyInsn;
			if (fieldInsn.getOpcode() != Opcodes.GETFIELD)
				throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": latency field insn opcode "+fieldInsn.getOpcode());
			if (!fieldInsn.desc.equals(Type.INT_TYPE.getDescriptor()))
				throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": latency field desc "+fieldInsn.desc);
			if (!fieldInsn.owner.equals(Type.getType(klass).getInternalName()))
				throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": latency field owner "+fieldInsn.owner);

			//Move latencyInsn to sync up with the other else-if branches.
			latencyInsn = latencyInsn.getPrevious();
			//We must be loading from this.
			if (latencyInsn.getOpcode() != Opcodes.ALOAD)
				throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": getfield subject opcode "+latencyInsn.getOpcode());
			int varIdx = ((VarInsnNode)latencyInsn).var;
			if (varIdx != 0)
				throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": getfield not from this but from "+varIdx);

			//Check the field we're loading from is constant (final).
			//A static field is okay here since it isn't a reference parameter.
			try {
				latencyField = klass.getDeclaredField(fieldInsn.name);
				if (!Modifier.isFinal(latencyField.getModifiers()))
					throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": latency field not final: "+latencyField.toGenericString());
			} catch (NoSuchFieldException ex) {
				throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": getfield not from this but from "+varIdx);
			}
		} else if (latencyInsn instanceof LdcInsnNode) {
			Object constant = ((LdcInsnNode)latencyInsn).cst;
			if (!(constant instanceof Integer))
				throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": ldc "+constant);
			constantLatency = ((Integer)constant);
		} else switch (latencyInsn.getOpcode()) {
			case Opcodes.ICONST_M1:
				constantLatency = -1;
				break;
			case Opcodes.ICONST_0:
				constantLatency = 0;
				break;
			case Opcodes.ICONST_1:
				constantLatency = 1;
				break;
			case Opcodes.ICONST_2:
				constantLatency = 2;
				break;
			case Opcodes.ICONST_3:
				constantLatency = 3;
				break;
			case Opcodes.ICONST_4:
				constantLatency = 4;
				break;
			case Opcodes.ICONST_5:
				constantLatency = 5;
				break;
			case Opcodes.BIPUSH:
			case Opcodes.SIPUSH:
				constantLatency = ((IntInsnNode)latencyInsn).operand;
				break;
			default:
				throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": latencyInsn opcode "+latencyInsn.getOpcode());
		}
		//Finally, we've parsed the latency parameter.

		//Next is an aload_0 for the sender parameter.
		AbstractInsnNode senderInsn = latencyInsn.getPrevious();
		if (senderInsn.getOpcode() != Opcodes.ALOAD || ((VarInsnNode)senderInsn).var != 0)
			throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": bad sender");

		//Finally, a getfield of this for a final Portal instance field.
		AbstractInsnNode portalInsn = senderInsn.getPrevious();
		if (!(portalInsn instanceof FieldInsnNode))
			throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": portal getfield opcode "+portalInsn.getOpcode());
		FieldInsnNode fieldInsn = (FieldInsnNode)portalInsn;
		if (fieldInsn.getOpcode() != Opcodes.GETFIELD)
			throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": portal field insn opcode "+fieldInsn.getOpcode());
		if (!fieldInsn.desc.equals(Type.getType(Portal.class).getDescriptor()))
			throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": portal field desc "+fieldInsn.desc);
		if (!fieldInsn.owner.equals(Type.getType(klass).getInternalName()))
			throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": portal field owner "+fieldInsn.owner);

		portalInsn = portalInsn.getPrevious();
		//We must be loading from this.
		if (portalInsn.getOpcode() != Opcodes.ALOAD)
			throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": portal getfield subject opcode "+portalInsn.getOpcode());
		int varIdx = ((VarInsnNode)portalInsn).var;
		if (varIdx != 0)
			throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": portal getfield not from this but from "+varIdx);

		//Check the field we're loading from is constant (final) and nonstatic.
		Field portalField;
		try {
			portalField = klass.getDeclaredField(fieldInsn.name);
			if (!Modifier.isFinal(portalField.getModifiers()))
				throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": portal field not final: "+portalField.toGenericString());
			if (Modifier.isStatic(portalField.getModifiers()))
				throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": portal field is static: "+portalField.toGenericString());
		} catch (NoSuchFieldException ex) {
			throw new IllegalStreamGraphException("Unsupported getHandle() use in "+klass+": portal getfield not from this but from "+varIdx);
		}

		return latencyField != null ? new WorkerData(portalField, latencyField) : new WorkerData(portalField, constantLatency);
	}
	//</editor-fold>

	/**
	 * Computes the SDEPData for the given edge between two workers, using the
	 * given cache of previously-computed SDEP data.  (The cache allows us to
	 * reuse common parts of the path from a sender to many recipients.)
	 * @param goalEdge the edge to compute for
	 * @param cache previously-computed SDEP data
	 * @return SDEPData for the given edge
	 */
	private static SDEPData computeSDEP(Edge goalEdge, Map<Edge, SDEPData> cache) {
		Set<Worker<?, ?>> allNodes = new NodesInPathsBetweenComputer(goalEdge.upstream, goalEdge.downstream).get();
		//TODO: see if NodesInPathsComputer adds these itself or not
		allNodes.add(goalEdge.upstream);
		allNodes.add(goalEdge.downstream);
		List<Worker<?, ?>> sortedNodes = Workers.topologicalSort(allNodes);
		//Add self-edges for all workers.
		for (Worker<?, ?> w : sortedNodes) {
			Edge selfEdge = new Edge(w, w);
			if (!cache.containsKey(selfEdge))
				cache.put(selfEdge, SDEPData.fromWorker(w));
		}
		//Add data-dependence edges for all workers.  If there's already data in
		//the cache, we know we must have added these data-dependency edges
		//before (and possibly merged them with some latency edges).
		for (int i = 0; i < sortedNodes.size(); ++i)
			for (int j = i+1; j < sortedNodes.size(); ++j) {
				if (Workers.getSuccessors(sortedNodes.get(i)).contains(sortedNodes.get(j))) {
					Edge edge = new Edge(sortedNodes.get(i), sortedNodes.get(j));
					if (!cache.containsKey(edge))
						cache.put(edge, SDEPData.fromDataDependence(edge.upstream, edge.downstream));
				}
			}

		//For each pair of nodes that follow one another, extend the edge from
		//the upstream through the pair of nodes, merging if such an edge is
		//already in the cache.  Because it's topologically ordered, we process
		//all upstream pairs before downstream pairs.
		for (int i = 0; i < sortedNodes.size(); ++i) {
			for (int j = i+1; j < sortedNodes.size(); ++j) {
				if (!Workers.getSuccessors(sortedNodes.get(i)).contains(sortedNodes.get(j)))
					continue;
				Edge upstreamEdge = new Edge(sortedNodes.get(0), sortedNodes.get(i));
				Edge downstreamEdge = new Edge(sortedNodes.get(j), sortedNodes.get(j));
				assert cache.containsKey(upstreamEdge) : "Bad topological sort?";
				SDEPData data = cache.get(upstreamEdge);
				assert cache.containsKey(downstreamEdge) : "Not caching self-edges?";
				SDEPData selfData = cache.get(downstreamEdge);

				Edge producedEdge = new Edge(upstreamEdge.upstream, downstreamEdge.downstream);
				SDEPData seriesData = SDEPData.combineSeries(data, selfData);

				SDEPData currentData = cache.get(producedEdge);
				if (currentData != null)
					cache.put(producedEdge, SDEPData.mergeParallel(currentData, seriesData));
				else
					cache.put(producedEdge, seriesData);
			}
		}

		assert cache.containsKey(goalEdge);
		return cache.get(goalEdge);
	}

	/**
	 * Encapsulates the data built during hierarchical SDEP computation for a
	 * particular pair of workers (not stored in the structure).
	 *
	 * Compare LatencyEdge in classic StreamIt.
	 */
	private static class SDEPData {
		private final int upstreamInitExecutions, upstreamSteadyExecutions;
		private final int downstreamInitExecutions, downstreamSteadyExecutions;
		/**
		 * The actual dependency function values.  In order for the downstream
		 * worker to execute i times, the upstream worker must have executed at
		 * least sdep[i] times.  Note that sdep[0] == 0.
		 *
		 * This array is (downstreamInitExecutions + downstreamSteadyExecutions + 1)
		 * in size; queries beyond that can be reduced to this region only
		 * because SDEP is periodic in the steady state.
		 */
		private final int[] sdep;
		private SDEPData(int upstreamInitExecutions, int upstreamSteadyExecutions, int downstreamInitExecutions, int downstreamSteadyExecutions, int[] sdep) {
			this.upstreamInitExecutions = upstreamInitExecutions;
			this.upstreamSteadyExecutions = upstreamSteadyExecutions;
			this.downstreamInitExecutions = downstreamInitExecutions;
			this.downstreamSteadyExecutions = downstreamSteadyExecutions;
			this.sdep = sdep;
		}

		/**
		 * Constructs SDEPData relating a worker to itself.
		 */
		public static SDEPData fromWorker(Worker<?, ?> worker) {
			//A plain worker has 0 init executions and 1 steady execution, and
			//an SDEP(1) of 1.  TODO: prework may mean 1 init execution?
			return new SDEPData(0, 1, 0, 1, new int[]{0, 1});
		}

		public static SDEPData fromDataDependence(Worker<?, ?> upstream, Worker<?, ?> downstream) {
			int uChannel = Workers.getSuccessors(upstream).indexOf(downstream);
			assert uChannel != -1;
			int dChannel = Workers.getPredecessors(downstream).indexOf(upstream);
			assert dChannel != -1;

			Rate pushRate = upstream.getPushRates().get(uChannel);
			Rate peekRate = downstream.getPeekRates().get(dChannel);
			Rate popRate = downstream.getPopRates().get(dChannel);
			//TODO: these exceptions should include the MessageConstraint!
			if (!pushRate.isFixed())
				throw new IllegalStreamGraphException("Messaging over dynamic rate", upstream);
			if (!peekRate.isFixed() || !popRate.isFixed())
				throw new IllegalStreamGraphException("Messaging over dynamic rate", downstream);

			int steadyStateData = lcm(pushRate.max(), popRate.max());
			int upstreamSteadyExecutions = steadyStateData/pushRate.max();
			int downstreamSteadyExecutions = steadyStateData/popRate.max();

			//Figure out how many upstream executions we need to fill peek
			//buffers to prepare for steady-state executions.  (This may be 0.)
			//Prework TODO: need to account for prework's peek and pop demands
			//Divide rounding up.
			int upstreamInitExecutions = (peekRate.max() - 1)/pushRate.max() + 1;
			//Always 0, at least until TODO prework.
			int downstreamInitExecutions = 0;

			//We know how much SDEP info we need, so simulate that many
			//downstream executions of a pull schedule between these two nodes.
			int[] sdep = new int[downstreamInitExecutions + downstreamSteadyExecutions + 1];
			int dataInChannel = 0;
			int upstreamExecutions = 0;
			int neededToFire = Math.max(peekRate.max(), popRate.max());
			for (int i = 1; i < sdep.length; ++i) {
				while (dataInChannel < neededToFire) {
					dataInChannel += pushRate.max();
					++upstreamExecutions;
				}
				dataInChannel -= popRate.max();
				sdep[i] = upstreamExecutions;
			}
			return new SDEPData(upstreamInitExecutions, upstreamSteadyExecutions, downstreamInitExecutions, downstreamSteadyExecutions, sdep);
		}

		/**
		 * Merge two edges that connect the same nodes.  (Used for taking the
		 * maximum over splitjoins.)
		 */
		public static SDEPData mergeParallel(SDEPData left, SDEPData right) {
			assert left != right;
			int downstreamInitExecutions = Math.max(left.downstreamInitExecutions, right.downstreamInitExecutions);
			int upstreamInitExecutions = Ints.checkedCast(Math.max(left.sdep(downstreamInitExecutions), right.sdep(downstreamInitExecutions)));

			int use1 = left.upstreamSteadyExecutions, use2 = right.upstreamSteadyExecutions;
			int dse1 = left.downstreamSteadyExecutions, dse2 = right.downstreamSteadyExecutions;
			//TODO: why only using use2/dse2?  because we do use1/dse1 * mult later?
			int uMult = use2 / IntMath.gcd(use1, use2);
			int dMult = dse2 / IntMath.gcd(dse1, dse2);
			int mult = uMult / IntMath.gcd(uMult, dMult) * dMult;
			int upstreamSteadyExecutions = use1 * mult;
			int downstreamSteadyExecutions = dse1 * mult;

			int[] sdep = new int[downstreamInitExecutions + downstreamSteadyExecutions + 1];
			for (int i = 0; i < sdep.length; ++i)
				sdep[i] = Ints.checkedCast(Math.max(left.sdep(i), right.sdep(i)));
			return new SDEPData(upstreamInitExecutions, upstreamSteadyExecutions, downstreamInitExecutions, downstreamSteadyExecutions, sdep);
		}

		/**
		 * Merge two edges that connect two different nodes.  (Pipelines.)
		 */
		public static SDEPData combineSeries(SDEPData upstream, SDEPData downstream) {
			int upstreamInitExecutions = Ints.checkedCast(Math.max(upstream.upstreamInitExecutions, upstream.sdep(downstream.upstreamInitExecutions)));
			int downstreamInitExecutions = Ints.checkedCast(Math.max(downstream.downstreamInitExecutions, upstream.reverseSdep(downstream.downstreamInitExecutions)));

			int gcd = IntMath.gcd(upstream.downstreamSteadyExecutions, downstream.upstreamSteadyExecutions);
			int uMult = downstream.upstreamSteadyExecutions / gcd;
			int dMult = upstream.downstreamSteadyExecutions / gcd;
			int upstreamSteadyExecutions = upstream.upstreamSteadyExecutions * uMult;
			int downstreamSteadyExecutions = downstream.downstreamSteadyExecutions * dMult;

			int[] sdep = new int[downstreamInitExecutions + downstreamSteadyExecutions + 1];
			for (int i = 0; i < sdep.length; ++i)
				sdep[i] = Ints.checkedCast(upstream.sdep(downstream.sdep(i)));
			return new SDEPData(upstreamInitExecutions, upstreamSteadyExecutions, downstreamInitExecutions, downstreamSteadyExecutions, sdep);
		}

		public long sdep(long downstreamExecutionCount) {
			if (downstreamExecutionCount < downstreamInitExecutions + 1)
				return sdep[Ints.checkedCast(downstreamExecutionCount)];
			long steadyStates = (downstreamExecutionCount - (downstreamInitExecutions + 1)) / downstreamSteadyExecutions;
			//Where we are in the current steady state, adjusted to ignore the
			//initialization prefix in the sdep array.
			int curSteadyStateProgress = Ints.checkedCast((downstreamExecutionCount - (downstreamInitExecutions + 1)) % downstreamSteadyExecutions + downstreamInitExecutions + 1);
			return sdep[curSteadyStateProgress] + steadyStates * upstreamSteadyExecutions;
		}

		public long reverseSdep(long upstreamExecutionCount) {
			//Factor out steady state executions, leaving upstreamExecutionCount
			//with only a partial steady state.
			long downstreamSteadyStateExecutions = 0;
			if (upstreamExecutionCount >= upstreamInitExecutions + upstreamSteadyExecutions + 1) {
				long steadyStates = (upstreamExecutionCount - upstreamInitExecutions - 1) / upstreamSteadyExecutions;
				downstreamSteadyStateExecutions = steadyStates * downstreamSteadyExecutions;
				upstreamExecutionCount -= steadyStates * upstreamSteadyExecutions;
			}

			//Find how many times the downstream executed during the
			//upstreamExecutionCount portion of the steady state.
			int downstreamExecutionCount = Arrays.binarySearch(sdep, Ints.checkedCast(upstreamExecutionCount));
			//Arrays.binarySearch doesn't guarantee which index it'll find, but
			//we want the first one.  If we didn't find one, this is a no-op.
			while (downstreamExecutionCount > 0 && sdep[downstreamExecutionCount] == upstreamExecutionCount)
				--downstreamExecutionCount;

			return downstreamSteadyStateExecutions + (downstreamExecutionCount >= 0 ? downstreamExecutionCount : downstreamInitExecutions + downstreamSteadyExecutions);
		}
	}

	/**
	 * Finds all nodes in any path between two nodes in the graph.
	 */
	private static class NodesInPathsBetweenComputer {
		private final Worker<?, ?> head, tail;
		private final Set<Worker<?, ?>> tailSuccessors;
		private final Map<Worker<?, ?>, Set<Worker<?, ?>>> nextNodesToTail = new HashMap<>();
		private NodesInPathsBetweenComputer(Worker<?, ?> head, Worker<?, ?> tail) {
			this.head = head;
			this.tail = tail;
			this.tailSuccessors = Workers.getAllSuccessors(tail);
		}
		public Set<Worker<?, ?>> get() {
			compute(head);
			Set<Worker<?, ?>> result = new HashSet<>();
			for (Set<Worker<?, ?>> nexts : nextNodesToTail.values())
				result.addAll(nexts);
			return result;
		}
		private boolean compute(Worker<?, ?> h) {
			if (h == tail)
				return true;
			Set<Worker<?, ?>> nodes = nextNodesToTail.get(h);
			if (nodes == null) {
				nodes = new HashSet<>();
				for (Worker<?, ?> next : Workers.getSuccessors(h)) {
					//If next is one of tail's successors, we can stop checking
					//this branch because we've gone too far down.
					if (tailSuccessors.contains(next))
						continue;
					//See if this node leads to tail.
					if (compute(next))
						nodes.add(next);
				}
				nextNodesToTail.put(h, nodes);
			}
			return !nodes.isEmpty();
		}
	}

	private static int lcm(int a, int b) {
		//Divide before multiplying for overflow resistance.
		return a / IntMath.gcd(a, b) * b;
	}

	/**
	 * A pair of workers, with equality based on the workers' identity.
	 */
	private static class Edge {
		public final Worker<?, ?> upstream, downstream;
		Edge(Worker<?, ?> upstream, Worker<?, ?> downstream) {
			this.upstream = upstream;
			this.downstream = downstream;
			StreamPosition direction = Workers.compareStreamPosition(upstream, downstream);
			assert direction == StreamPosition.UPSTREAM || direction == StreamPosition.EQUAL;
		}
		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final Edge other = (Edge)obj;
			if (this.upstream != other.upstream)
				return false;
			if (this.downstream != other.downstream)
				return false;
			return true;
		}
		@Override
		public int hashCode() {
			int hash = 3;
			hash = 23 * hash + System.identityHashCode(this.upstream);
			hash = 23 * hash + System.identityHashCode(this.downstream);
			return hash;
		}
	}
}
