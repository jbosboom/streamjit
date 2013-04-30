package edu.mit.streamjit.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/30/2013
 */
public final class TopologicalSort {
	private TopologicalSort() {}

	public interface PartialOrder<T> {
		public boolean lessThan(T a, T b);
	}

	public static <T> ImmutableList<T> sort(Iterable<T> data, PartialOrder<? super T> order) {
		return sort(data.iterator(), order);
	}

	public static <T> ImmutableList<T> sort(Iterator<T> iterator, PartialOrder<? super T> order) {
		//A bubble sort.
		List<T> list = Lists.newArrayList(iterator);
		boolean progress;
		do {
			progress = false;
			for (int i = 0; i < list.size(); ++i)
				for (int j = i+1; j < list.size(); ++j)
					if (order.lessThan(list.get(j), list.get(i))) {
						Collections.swap(list, i, j);
						progress = true;
					}
		} while (progress);
		return ImmutableList.copyOf(list);
	}
}
