package edu.mit.streamjit.impl.blob;

/**
 * AbstractBuffer implements some Buffer methods in terms of read() and write().
 * Custom implementations, when supported by the underlying storage, will
 * generally yield better performance.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 7/27/2013
 */
public abstract class AbstractBuffer implements Buffer {
	@Override
	public int read(Object[] data, int offset, int length) {
		int read = 0;
		Object obj;
		while (read < length && (obj = read()) != null)
			data[offset++] = obj;
		return read;
	}

	@Override
	public boolean readAll(Object[] data) {
		return readAll(data, 0);
	}

	@Override
	public boolean readAll(Object[] data, int offset) {
		int required = data.length - offset;
		if (required > size())
			return false;
		for (; offset < data.length; ++offset) {
			Object e = read();
			//We checked size() above, so we should never fail here, except in
			//case of concurrent modification by another reader.
			assert e != null;
			data[offset] = e;
		}
		return true;
	}

	@Override
	public int write(Object[] data, int offset, int length) {
		int written = 0;
		while (written < length && write(data[offset++]))
			++written;
		return written;
	}
}
