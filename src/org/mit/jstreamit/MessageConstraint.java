package org.mit.jstreamit;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.mit.jstreamit.PrimitiveWorker.StreamPosition;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
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
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/29/2013
 */
final class MessageConstraint {
	private final PrimitiveWorker<?, ?> sender, recipient;
	private final int latency;
	private final PrimitiveWorker.StreamPosition direction;
	//TODO: the actual restriction on execution...
	private MessageConstraint(PrimitiveWorker<?, ?> sender, PrimitiveWorker<?, ?> recipient, int latency, StreamPosition direction) {
		this.sender = sender;
		this.recipient = recipient;
		this.latency = latency;
		this.direction = direction;
	}
	public PrimitiveWorker<?, ?> getSender() {
		return sender;
	}
	public PrimitiveWorker<?, ?> getRecipient() {
		return recipient;
	}
	public int getLatency() {
		return latency;
	}
	public PrimitiveWorker.StreamPosition getDirection() {
		return direction;
	}
	@Override
	public String toString() {
		return String.format("%s from %s to %s after %d", direction, sender, recipient, latency);
	}

	/**
	 * Grovels through the stream graph, discovering message constraints.
	 * @param graph
	 * @return
	 */
	public static List<MessageConstraint> findConstraints(PrimitiveWorker<?, ?> graph) {
		List<MessageConstraint> mc = new ArrayList<>();
		List<PrimitiveWorker<?, ?>> workers = new ArrayList<>();
		workers.add(graph);
		workers.addAll(graph.getAllSuccessors());
		//Parsing bytecodes is (relatively) expensive; we only want to do it
		//once per class, no matter how many instances are in the stream graph.
		//If a class doesn't send messages, it maps to an empty list, and we do
		//nothing in the loop below.
		Map<Class<?>, List<WorkerData>> workerDataCache = new HashMap<>();

		for (PrimitiveWorker<?, ?> sender : workers) {
			List<WorkerData> datas = workerDataCache.get(sender.getClass());
			if (datas == null) {
				datas = buildWorkerData(sender);
				workerDataCache.put(sender.getClass(), datas);
			}

			for (WorkerData d : datas) {
				int latency = d.getLatency(sender);
				for (PrimitiveWorker<?, ?> recipient : d.getPortal(sender).getRecipients())
					mc.add(new MessageConstraint(sender, recipient, latency, sender.compareStreamPosition(recipient)));
			}
		}

		return Collections.unmodifiableList(mc);
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
		public Portal<?> getPortal(PrimitiveWorker<?, ?> worker) {
			try {
				return (Portal<?>)portalField.get(worker);
			} catch (IllegalAccessException | IllegalArgumentException | NullPointerException | ExceptionInInitializerError ex) {
				throw new AssertionError("getting a portal object", ex);
			}
		}
		public int getLatency(PrimitiveWorker<?, ?> worker) {
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

	private static List<WorkerData> buildWorkerData(PrimitiveWorker<?, ?> worker) {
		Class<?> klass = worker.getClass();
		//A worker can only send messages if it has a Portal field, and most
		//workers with Portal fields will send messages, so this is an efficient
		//and useful test to avoid the bytecode parse.
		if (!hasPortalField(worker.getClass()))
			return Collections.emptyList();
		return parseBytecodes(klass);
	}

	private static boolean hasPortalField(Class<?> klass) {
		while (klass != null) {
			for (Field f : klass.getDeclaredFields())
				if (f.getType().equals(Portal.class))
					return true;
			for (Class<?> i : klass.getInterfaces())
				for (Field f : i.getDeclaredFields())
					if (f.getType().equals(Portal.class))
						return true;
			klass = klass.getSuperclass();
		}
		return false;
	}

	/**
	 * Parse the given class' bytecodes, looking for calls to getHandle() and
	 * returning WorkerDatas holding the calls' arguments.
	 * @param klass
	 * @return
	 */
	private static List<WorkerData> parseBytecodes(Class<?> klass) {
		ClassReader r = null;
		try {
			r = new ClassReader(klass.getName());
		} catch (IOException ex) {
			throw new IllegalStreamGraphException("Couldn't get bytecode for "+klass.getName(), ex);
		}

		WorkClassVisitor wcv = new WorkClassVisitor();
		r.accept(wcv, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
		MethodNode mn = wcv.getWorkMethodNode();

		List<WorkerData> workerDatas = new ArrayList<>();
		for (AbstractInsnNode insn = mn.instructions.getFirst(); insn != null; insn = insn.getNext()) {
			if (insn instanceof MethodInsnNode) {
				MethodInsnNode call = (MethodInsnNode)insn;
				if (call.name.equals("getHandle") && call.owner.equals(Type.getType(Portal.class).getInternalName()))
					workerDatas.add(dataFromCall(klass, call));
			}
		}

		return workerDatas.isEmpty() ? Collections.<WorkerData>emptyList() : Collections.unmodifiableList(workerDatas);
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

	/**
	 * Builds a MethodNode for the work() method.
	 */
	private static class WorkClassVisitor extends ClassVisitor {
		private MethodNode mn;
		WorkClassVisitor() {
			super(Opcodes.ASM4);
		}
		@Override
		public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
			if (name.equals("work") && desc.equals("()V")) {
				mn = new MethodNode(Opcodes.ASM4, access, name, desc, signature, exceptions);
				return mn;
			}
			return null;
		}
		public MethodNode getWorkMethodNode() {
			return mn;
		}
	}
	//</editor-fold>

	/**
	 * Finds all nodes in any path between two nodes in the graph.
	 */
	private static class NodesInPathsBetweenComputer {
		private final PrimitiveWorker<?, ?> head, tail;
		private final Set<PrimitiveWorker<?, ?>> tailSuccessors;
		private final Map<PrimitiveWorker<?, ?>, Set<PrimitiveWorker<?, ?>>> nextNodesToTail = new HashMap<>();
		private NodesInPathsBetweenComputer(PrimitiveWorker<?, ?> head, PrimitiveWorker<?, ?> tail) {
			this.head = head;
			this.tail = tail;
			this.tailSuccessors = tail.getAllSuccessors();
		}
		public Set<PrimitiveWorker<?, ?>> get() {
			compute(head);
			Set<PrimitiveWorker<?, ?>> result = new HashSet<>();
			for (Set<PrimitiveWorker<?, ?>> nexts : nextNodesToTail.values())
				result.addAll(nexts);
			return result;
		}
		private boolean compute(PrimitiveWorker<?, ?> h) {
			if (h == tail)
				return true;
			Set<PrimitiveWorker<?, ?>> nodes = nextNodesToTail.get(h);
			if (nodes == null) {
				nodes = new HashSet<>();
				for (PrimitiveWorker<?, ?> next : h.getSuccessors()) {
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

	/**
	 * Topologically sort the given set of nodes, such that each node precedes
	 * all of its successors in the returned list.
	 * @param nodes the set of nodes to sort
	 * @return a topologically-ordered list of the given nodes
	 */
	private static List<PrimitiveWorker<?, ?>> topologicalSort(Set<PrimitiveWorker<?, ?>> nodes) {
		//Build a "use count" for each node, counting the number of nodes that
		//have it as a successor.
		Map<PrimitiveWorker<?, ?>, Integer> useCount = new HashMap<>();
		for (PrimitiveWorker<?, ?> n : nodes)
			useCount.put(n, 0);
		for (PrimitiveWorker<?, ?> n : nodes)
			for (PrimitiveWorker<?, ?> next : n.getSuccessors()) {
				Integer count = useCount.get(next);
				if (count != null)
					useCount.put(next, count+1);
			}

		List<PrimitiveWorker<?, ?>> result = new ArrayList<>();
		Queue<PrimitiveWorker<?, ?>> unused = new ArrayDeque<>();
		for (Map.Entry<PrimitiveWorker<?, ?>, Integer> e : useCount.entrySet())
			if (e.getValue() == 0)
				unused.add(e.getKey());
		while (!unused.isEmpty()) {
			PrimitiveWorker<?, ?> n = unused.remove();
			result.add(n);
			//Decrement the use counts of n's successors, adding them to unused
			//if the use count becomes zero.
			for (PrimitiveWorker<?, ?> next : n.getSuccessors()) {
				Integer count = useCount.get(next);
				if (count != null) {
					count -= 1;
					useCount.put(next, count);
					if (count == 0)
						unused.add(next);
				}
			}
		}

		assert result.size() == nodes.size();
		return result;
	}
}
