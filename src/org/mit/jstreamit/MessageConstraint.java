package org.mit.jstreamit;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mit.jstreamit.PrimitiveWorker.StreamPosition;
import org.objectweb.asm.ClassReader;

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

	private static List<WorkerData> parseBytecodes(Class<?> klass) {
		ClassReader r = null;
		try {
			r = new ClassReader(klass.getCanonicalName());
		} catch (IOException ex) {
			throw new IllegalStreamGraphException("Couldn't get bytecode for "+klass.getCanonicalName());
		}

		//TODO: the hard part!

		return Collections.emptyList();
	}
}
