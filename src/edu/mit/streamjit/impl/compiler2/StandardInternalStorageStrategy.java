package edu.mit.streamjit.impl.compiler2;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import java.util.Set;

/**
 * The standard internal storage factory, for when not tuning.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/1/2014
 */
public final class StandardInternalStorageStrategy implements StorageStrategy {
	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		//no parameters necessary
	}
	@Override
	public StorageFactory asFactory(Configuration config) {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				Arrayish array = new Arrayish.ArrayArrayish(storage.type(), storage.steadyStateCapacity());
				return new InternalArrayConcreteStorage(array, storage);
			}
		};
	}
}
