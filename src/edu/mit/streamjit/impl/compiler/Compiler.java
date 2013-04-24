package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableSet;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Rate;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StatefulFilter;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.IOInfo;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.impl.compiler.insts.CallInst;
import edu.mit.streamjit.impl.compiler.insts.ReturnInst;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
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
	private final Map<Worker<?, ?>, WorkerData> workerData;
	private final String packagePrefix;
	private final Module module = new Module();
	private final Klass blobKlass;
	public Compiler(Set<Worker<?, ?>> workers, Configuration config, int maxNumCores) {
		this.workers = workers;
		this.config = config;
		this.maxNumCores = maxNumCores;
		this.ioinfo = IOInfo.create(workers);
		this.workerData = new IdentityHashMap<>(workers.size());

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
	}

	public Blob compile() {
		for (Worker<?, ?> w : workers)
			buildWorkerData(w);
		addBlobPlumbing();
		return instantiateBlob();
	}

	private void buildWorkerData(Worker<?, ?> worker) {
		WorkerData data = new WorkerData(worker);
		workerData.put(worker, data);
		int id = Workers.getIdentifier(worker);
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
					"w"+id+"$"+f.getName(),
					modifiers,
					blobKlass);
			data.fields.put(f, nf);

			try {
				rf.setAccessible(true);
				Object value = rf.get(worker);
				data.fieldValues.put(f, value);
			} catch (IllegalAccessException ex) {
				//Either setAccessible will succeed or we'll throw a
				//SecurityException, so we'll never get here.
				throw new AssertionError("Can't happen!", ex);
			}
		}
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

	/**
	 * WorkerData contains worker-specific information.
	 */
	private static final class WorkerData {
		private final Worker<?, ?> worker;
		/**
		 * The method corresponding to this worker's work method.  May be null
		 * if the method hasn't been created yet.
		 */
		private Method workMethod;
		/**
		 * Maps fields in the worker class to fields in the blob class for this
		 * particular worker.
		 */
		private final Map<Field, Field> fields = new IdentityHashMap<>();
		/**
		 * Maps final fields in the worker class to their actual values, for
		 * inlining or blob initialization purposes.
		 */
		private final Map<Field, Object> fieldValues = new IdentityHashMap<>();
		private WorkerData(Worker<?, ?> worker) {
			this.worker = worker;
		}
	}

	public static void main(String[] args) {
		OneToOneElement<Integer, Integer> graph = new Splitjoin<>(new RoundrobinSplitter<Integer>(), new RoundrobinJoiner<Integer>(), new Identity<Integer>(), new Identity<Integer>());
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		graph.visit(cwv);
		Set<Worker<?, ?>> workers = Workers.getAllWorkersInGraph(cwv.getSource());
		Configuration config = Configuration.builder().build();
		int maxNumCores = 1;
		Compiler compiler = new Compiler(workers, config, maxNumCores);
		Blob blob = compiler.compile();
		compiler.blobKlass.dump(new PrintWriter(System.out, true));
		blob.getCoreCount();
	}
}
