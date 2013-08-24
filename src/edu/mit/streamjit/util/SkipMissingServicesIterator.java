package edu.mit.streamjit.util;

import com.google.common.collect.AbstractIterator;
import java.util.Iterator;
import java.util.ServiceConfigurationError;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This iterator wraps an iterator and skips over any next() calls that complete
 * abruptly due to ServiceConfigurationError if the named provider does not
 * exist.
 *
 * This class exists because annotation processors supporting incremental
 * compilation are not able to remove a provider from META-INF/services when its
 * annotation is removed.  This iterator simulates their removal by skipping
 * those errors (but only those errors -- if the provider isn't instantiable,
 * for example, the error will still be thrown).
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 8/22/2013
 */
public class SkipMissingServicesIterator<T> extends AbstractIterator<T> {
	private static final Pattern NOT_FOUND_MESSAGE = Pattern.compile("Provider (.*) not found$");
	private final Iterator<? extends T> iterator;
	public SkipMissingServicesIterator(Iterator<? extends T> iterator) {
		this.iterator = iterator;
	}
	@Override
	protected T computeNext() {
		while (iterator.hasNext()) {
			try {
				return iterator.next();
			} catch (ServiceConfigurationError ex) {
				Matcher m = NOT_FOUND_MESSAGE.matcher(ex.getMessage());
				if (m.find())
					continue; //TODO: verify the provider doesn't exist? Do we trust ServiceLoader?
				throw ex;
			}
		}
		return endOfData();
	}
}
