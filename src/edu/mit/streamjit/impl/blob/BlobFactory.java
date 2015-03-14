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
package edu.mit.streamjit.impl.blob;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import java.util.Set;

/**
 * A BlobFactory creates Blobs.
 *
 * Instances of this class should be stateless and immutable; they must at least
 * be thread-safe and reentrant.  As instances of this class may be used in
 * configurations, they should implement equals() and hashCode().
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 3/26/2013
 */
public interface BlobFactory {
	/**
	 * Creates a new Blob responsible for the given set of workers using the
	 * given configuration that will run on at most the given number of cores.
	 *
	 * Not all factories can make blobs for arbitrary sets of workers, so
	 * callers should be prepared to handle IllegalArgumentException.
	 *
	 * Some
	 * factories may require particular information in their configuration and
	 * may throw if it is not present or is of a form they do not understand
	 * (such as a parameter being the wrong type). However, factories must not throw just
	 * because they were given configuration parameters they do not know about;
	 * instead, they must ignore the extra information.
	 *
	 * Factories need not produce blobs that utilize all of their allotted
	 * cores; callers should use the getCoreCount() method of the returned Blob
	 * to decide how many threads to start, etc.
	 * @param workers the set of workers to be placed in the blob (must be
	 * nonempty)
	 * @param config the configuration used to make a blob
	 * @param maxNumCores the maximum number of cores the blob may use (must be
	 * >= 1)
	 * @return a Blob responsible for the given workers
	 */
	public Blob makeBlob(Set<Worker<?, ?>> workers, Configuration config, int maxNumCores, DrainData initialState);

	/**
	 * Creates a new Configuration with parameters and default values suitable
	 * for running (and autotuning) the given set of workers using the blob type
	 * produced by this BlobFactory. This method is necessary so that the
	 * autotuner has a starting point.
	 * <p/>
	 * The set of workers should generally be the whole stream graph (see
	 * Workers.getAllWorkersInGraph()).
	 * @param workers a set of workers, usually the whole stream graph
	 * @return a default configuration for this blob type
	 */
	public Configuration getDefaultConfiguration(Set<Worker<?, ?>> workers);
}
