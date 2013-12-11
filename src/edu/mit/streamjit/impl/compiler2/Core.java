package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Range;
import edu.mit.streamjit.util.CollectionUtils;
import edu.mit.streamjit.util.Combinators;
import edu.mit.streamjit.util.Pair;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Represents one core during the compilation.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/17/2013
 */
public class Core {
	private final ImmutableMap<Storage, ConcreteStorage> globalStorage, localStorage, allStorage;
	private final ImmutableTable<Actor, Integer, IndexFunctionTransformer> inputTransformers, outputTransformers;
	private final List<Pair<ActorGroup, Range<Integer>>> allocations = new ArrayList<>();
	public Core(Set<Storage> storage, ImmutableMap<Storage, ConcreteStorage> globalStorage,
			StorageFactory localStorageFactory,
			ImmutableTable<Actor, Integer, IndexFunctionTransformer> inputTransformers,
			ImmutableTable<Actor, Integer, IndexFunctionTransformer> outputTransformers) {
		this.globalStorage = globalStorage;
		//We make ConcreteStorage for every local storage, despite not knowing
		//what we need yet; anything we don't use will get garbage collected
		//when the compiler state is discarded after instantiating the Blob.
		//TODO: this wastes space if we're assigned only part of a fissed
		//schedule; we should wait until we're building code and can compute
		//the space requirement.
		ImmutableMap.Builder<Storage, ConcreteStorage> localStorageBuilder = ImmutableMap.builder();
		for (Storage s : storage)
			if (s.isInternal())
				localStorageBuilder.put(s, localStorageFactory.make(s));
		this.localStorage = localStorageBuilder.build();
		this.allStorage = CollectionUtils.union(globalStorage, localStorage);
		this.inputTransformers = inputTransformers;
		this.outputTransformers = outputTransformers;
	}

	public void allocate(ActorGroup group, Range<Integer> iterations) {
		allocations.add(Pair.make(group, iterations));
	}

	public MethodHandle code() {
		//TODO: here's where to plug in ActorGroup ordering parameters
		List<MethodHandle> code = new ArrayList<>(allocations.size());
		for (Pair<ActorGroup, Range<Integer>> p : allocations)
			code.add(p.first.specialize(p.second, allStorage, inputTransformers, outputTransformers));
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

	/**
	 * Returns the ConcreteStorage instances allocated for internal Storage by
	 * this core.
	 * @return local ConcreteStorage instances
	 */
	public ImmutableMap<Storage, ConcreteStorage> localStorage() {
		return allStorage;
	}

	@Override
	public String toString() {
		return allocations.toString();
	}
}
