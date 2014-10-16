package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Range;
import edu.mit.streamjit.util.bytecode.methodhandles.Combinators;
import edu.mit.streamjit.util.Pair;
import edu.mit.streamjit.util.bytecode.methodhandles.ProxyFactory;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents one core during the compilation.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/17/2013
 */
public class Core {
	private final ImmutableMap<Storage, ConcreteStorage> storage;
	private final ImmutableMap<ActorGroup, Integer> unrollFactors;
	private final ImmutableTable<Actor, Integer, IndexFunctionTransformer> inputTransformers, outputTransformers;
	private final ProxyFactory bytecodifier;
	private final List<Pair<ActorGroup, Range<Integer>>> allocations = new ArrayList<>();
	public Core(ImmutableMap<Storage, ConcreteStorage> storage,
			ImmutableMap<ActorGroup, Integer> unrollFactors,
			ImmutableTable<Actor, Integer, IndexFunctionTransformer> inputTransformers,
			ImmutableTable<Actor, Integer, IndexFunctionTransformer> outputTransformers,
			ProxyFactory bytecodifier) {
		this.storage = storage;
		this.unrollFactors = unrollFactors;
		this.inputTransformers = inputTransformers;
		this.outputTransformers = outputTransformers;
		this.bytecodifier = bytecodifier;
	}

	public void allocate(ActorGroup group, Range<Integer> iterations) {
		if (!iterations.isEmpty())
			allocations.add(Pair.make(group, iterations));
	}

	public MethodHandle code() {
		//TODO: ActorGroup ordering parameters: accumulate a
		//List<Pair<ActorGroup, MethodHandle>>, then sort before semicolon(code).
		List<MethodHandle> code = new ArrayList<>(allocations.size());
		for (Pair<ActorGroup, Range<Integer>> p : allocations)
			code.add(p.first.specialize(p.second, storage, unrollFactors.get(p.first), inputTransformers, outputTransformers, bytecodifier));
		return Combinators.semicolon(code);
	}

	/**
	 * Returns true iff this Core is empty (has no allocations) and thus doesn't
	 * need to generate or run code.
	 * @return true iff this core is empty
	 */
	public boolean isEmpty() {
		return allocations.isEmpty();
	}

	@Override
	public String toString() {
		return allocations.toString();
	}
}
