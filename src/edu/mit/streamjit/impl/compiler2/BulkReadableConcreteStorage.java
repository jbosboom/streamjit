package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.impl.blob.Buffer;

/**
 * A ConcreteStorage that can write data items to an output Buffer in bulk (from
 * the ConcreteStorage's point of view, a bulk read operation).  Because Blobs
 * must perform short writes to avoid deadlock in multi-Blob graphs,
 * implementations and users must cope with short writes.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 12/5/2013
 */
public interface BulkReadableConcreteStorage extends ConcreteStorage {
	/**
	 * Copies {@code count} elements starting at {@code index} from this
	 * ConcreteStorage to {@code dest}.
	 * @param dest the buffer to copy to
	 * @param index the index of the first element to copy
	 * @param count the number of element to copy
	 * @return the number of items copied
	 */
	public int bulkRead(Buffer dest, int index, int count);
}
