package edu.mit.streamjit.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import java.util.Map;

/**
 * Contains collection related utilities not in {@link java.util.Collections}
 * or the Guava collection utilities.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
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
}
