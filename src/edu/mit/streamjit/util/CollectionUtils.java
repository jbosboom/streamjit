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
package edu.mit.streamjit.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import java.util.List;
import java.util.Map;

/**
 * Contains collection related utilities not in {@link java.util.Collections}
 * or the Guava collection utilities.
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 10/22/2013
 */
public final class CollectionUtils {
	private CollectionUtils() {}

	/**
	 * Returns the union of the given maps with disjoint key sets.
	 * @param <K> the key type of the returned map
	 * @param <V> the value type of the returned map
	 * @param first the first map
	 * @param more more maps
	 * @return a map containing all the entries in the given maps
	 */
	@SafeVarargs
	public static <K, V> ImmutableMap<K, V> union(Map<? extends K, ? extends V> first, Map<? extends K, ? extends V>... more) {
		ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
		builder.putAll(first);
		for (Map<? extends K, ? extends V> m : more)
			builder.putAll(m);
		return builder.build();
	}

	/**
	 * Returns the union of the given maps, using the given function to merge
	 * values for the same key.  The function is called for all keys with a list
	 * of the values of the maps in the order the maps were given.  Maps that do
	 * not contain the key are not represented in the list.  The function's
	 * return value is used as the value in the union map.
	 * @param <K> the key type of the returned map
	 * @param <V> the value type of the returned map
	 * @param <X> the value type of the input map(s)
	 * @param merger the function used to merge values for the same key
	 * @param first the first map
	 * @param more more maps
	 * @return a map containing all the keys in the given maps
	 */
	@SafeVarargs
	public static <K, V, X> ImmutableMap<K, V> union(Maps.EntryTransformer<? super K, ? super List<? super X>, ? extends V> merger, Map<? extends K, ? extends X> first, Map<? extends K, ? extends X>... more) {
		return union(merger, Lists.asList(first, more));
	}

	/**
	 * Returns the union of the given maps, using the given function to merge
	 * values for the same key.  The function is called for all keys with a list
	 * of the values of the maps in the order the maps were given.  Maps that do
	 * not contain the key are not represented in the list.  The function's
	 * return value is used as the value in the union map.
	 * TODO: the generics don't seem right here; I should be able to use e.g.
	 * a Collection<Comparable> in place of List<Integer> for the middle arg.
	 * Note that the above overload permits that and forwards to this one!
	 * @param <K> the key type of the returned map
	 * @param <V> the value type of the returned map
	 * @param <X> the value type of the input map(s)
	 * @param merger the function used to merge values for the same key
	 * @param maps the maps
	 * @return a map containing all the keys in the given maps
	 */
	public static <K, V, X> ImmutableMap<K, V> union(Maps.EntryTransformer<? super K, ? super List<X>, ? extends V> merger, List<? extends Map<? extends K, ? extends X>> maps) {
		ImmutableSet.Builder<K> keys = ImmutableSet.builder();
		for (Map<? extends K, ? extends X> m : maps)
			keys.addAll(m.keySet());

		ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
		for (K k : keys.build()) {
			ImmutableList.Builder<X> values = ImmutableList.builder();
			for (Map<? extends K, ? extends X> m : maps)
				if (m.containsKey(k))
					values.add(m.get(k));
			builder.put(k, merger.transformEntry(k, values.build()));
		}
		return builder.build();
	}

	/**
	 * Returns the union of the given tables with disjoint key sets.
	 * @param <R> the row type of the returned table
	 * @param <C> the column type of the returned table
	 * @param <V> the value type of the returned table
	 * @param first the first table
	 * @param more more tables
	 * @return a table containing all the cells in the given tables
	 */
	@SafeVarargs
	public static <R, C, V> ImmutableTable<R, C, V> union(Table<? extends R, ? extends C, ? extends V> first, Table<? extends R, ? extends C, ? extends V>... more) {
		ImmutableTable.Builder<R, C, V> builder = ImmutableTable.builder();
		builder.putAll(first);
		for (Table<? extends R, ? extends C, ? extends V> t : more)
			builder.putAll(t);
		return builder.build();
	}

	/**
	 * Returns the index of the minimum element in the list, using the elements'
	 * natural ordering, or -1 if the list is empty.
	 * @param list a list
	 * @return the index of the minimum element
	 */
	public static <T extends Comparable<? super T>> int minIndex(List<T> list) {
		if (list.isEmpty())
			return -1;
		if (list.size() == 1)
			return 0;
		int best = 0;
		//TODO: alternative implementation for non-RandomAccess lists?
		for (int i = 1; i < list.size(); ++i)
			if (list.get(i).compareTo(list.get(best)) < 0)
				best = i;
		return best;
	}

	/**
	 * Returns the index of the maximum element in the list, using the elements'
	 * natural ordering, or -1 if the list is empty.
	 * @param list a list
	 * @return the index of the maximum element
	 */
	public static <T extends Comparable<? super T>> int maxIndex(List<T> list) {
		if (list.isEmpty())
			return -1;
		if (list.size() == 1)
			return 0;
		int best = 0;
		//TODO: alternative implementation for non-RandomAccess lists?
		for (int i = 1; i < list.size(); ++i)
			if (list.get(i).compareTo(list.get(best)) > 0)
				best = i;
		return best;
	}
}
