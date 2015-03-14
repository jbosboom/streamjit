/*
 * Copyright (c) 2013-2015 Massachusetts Institute of Technology
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
package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Range;
import edu.mit.streamjit.util.bytecode.methodhandles.Combinators;
import edu.mit.streamjit.util.Pair;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Represents one core during the compilation.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 10/17/2013
 */
public class Core {
	private final ImmutableMap<Storage, ConcreteStorage> storage;
	private final BiFunction<MethodHandle[], WorkerActor, MethodHandle> switchFactory;
	private final ImmutableMap<ActorGroup, Integer> unrollFactors;
	private final ImmutableTable<Actor, Integer, IndexFunctionTransformer> inputTransformers, outputTransformers;
	private final List<Pair<ActorGroup, Range<Integer>>> allocations = new ArrayList<>();
	public Core(ImmutableMap<Storage, ConcreteStorage> storage,
			BiFunction<MethodHandle[], WorkerActor, MethodHandle> switchFactory,
			ImmutableMap<ActorGroup, Integer> unrollFactors,
			ImmutableTable<Actor, Integer, IndexFunctionTransformer> inputTransformers,
			ImmutableTable<Actor, Integer, IndexFunctionTransformer> outputTransformers) {
		this.storage = storage;
		this.switchFactory = switchFactory;
		this.unrollFactors = unrollFactors;
		this.inputTransformers = inputTransformers;
		this.outputTransformers = outputTransformers;
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
			code.add(p.first.specialize(p.second, storage, switchFactory, unrollFactors.get(p.first), inputTransformers, outputTransformers));
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
