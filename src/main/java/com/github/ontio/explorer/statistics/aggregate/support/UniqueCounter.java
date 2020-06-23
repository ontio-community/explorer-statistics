package com.github.ontio.explorer.statistics.aggregate.support;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author LiuQi
 */
public interface UniqueCounter<T> {

	void count(T... values);

	int getCount();

	class SimpleUniqueCounter<T extends Comparable<T>> implements UniqueCounter<T> {

		private Set<T> set = new HashSet<>();

		@Override
		@SafeVarargs
		public final void count(T... values) {
			if (values != null) {
				set.addAll(Arrays.asList(values));
			}
		}

		@Override
		public int getCount() {
			return set.size();
		}

	}

}
