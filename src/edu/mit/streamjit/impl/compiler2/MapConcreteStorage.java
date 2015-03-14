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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import edu.mit.streamjit.util.bytecode.methodhandles.Combinators;
import static edu.mit.streamjit.util.bytecode.methodhandles.LookupUtils.findVirtual;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A ConcreteStorage implementation using a Map<Integer, T>.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 10/27/2013
 */
public final class MapConcreteStorage implements ConcreteStorage {
	private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
	private static final MethodHandle MAP_GET = findVirtual(Map.class, "get")
			.asType(MethodType.methodType(Object.class, Map.class, int.class));
	private static final MethodHandle MAP_PUT = findVirtual(Map.class, "put")
			.asType(MethodType.methodType(void.class, Map.class, int.class, Object.class));
	private static final MethodHandle ADJUST = findVirtual(LOOKUP, "adjust");
	private final Class<?> type;
	private final Map<Integer, Object> map = new ConcurrentHashMap<>();
	private final MethodHandle readHandle, writeHandle, adjustHandle;
	private final int minReadIndex, throughput;
	private MapConcreteStorage(Class<?> type, MethodHandle adjustHandle, int minReadIndex, int throughput) {
		this.type = type;
		this.readHandle = MAP_GET.bindTo(map).asType(MethodType.methodType(type, int.class));
		this.writeHandle = MAP_PUT.bindTo(map).asType(MethodType.methodType(void.class, int.class, type));
		this.adjustHandle = adjustHandle.bindTo(this);
		this.minReadIndex = minReadIndex;
		this.throughput = throughput;
	}
	public static MapConcreteStorage create(Storage s) {
		ImmutableSet<ActorGroup> relevantGroups = ImmutableSet.<ActorGroup>builder().addAll(s.upstreamGroups()).addAll(s.downstreamGroups()).build();
		return new MapConcreteStorage(s.type(), ADJUST, s.readIndices(Maps.asMap(relevantGroups, i -> 1)).first(), s.throughput());
	}
	public static MapConcreteStorage createNopAdjust(Storage s) {
		return new MapConcreteStorage(s.type(), Combinators.nop(Object.class), Integer.MAX_VALUE, Integer.MAX_VALUE);
	}
	//TODO: create(Storage s, Map<ActorGroup, Integer> schedule) if we want adjusting

	@Override
	public Class<?> type() {
		return type;
	}

	@Override
	public void adjust() {
		//This is pretty slow, but we need them in order so we don't overwrite.
		ImmutableSortedSet<Integer> indices = ImmutableSortedSet.copyOf(map.keySet());
		for (int i : indices) {
			int newReadIndex = i - throughput;
			Object item = map.remove(i);
			if (newReadIndex >= minReadIndex) {
				Object overwrote = map.put(newReadIndex, item);
				assert overwrote == null : newReadIndex;
			}
		}
	}

	@Override
	public void sync() {
	}

	@Override
	public MethodHandle readHandle() {
		return readHandle;
	}

	@Override
	public MethodHandle writeHandle() {
		return writeHandle;
	}

	@Override
	public MethodHandle adjustHandle() {
		return adjustHandle;
	}

	@Override
	public String toString() {
		return map.toString();
	}

	public static StorageFactory factory() {
		return MapConcreteStorage::create;
	}

	public static StorageFactory initFactory() {
		return MapConcreteStorage::createNopAdjust;
	}
}
