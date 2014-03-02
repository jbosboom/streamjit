package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableList;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.IOInfo;
import java.util.Set;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/1/2014
 */
public final class TuneInternalStorageStrategy implements StorageStrategy {
	private final ImmutableList<Arrayish.Factory> ARRAYISH_FACTORIES = ImmutableList.of(
			Arrayish.ArrayArrayish.factory(),
			Arrayish.NIOArrayish.factory(),
			Arrayish.UnsafeArrayish.factory()
	);
	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		for (IOInfo i : IOInfo.allEdges(workers)) {
			builder.addParameter(new Configuration.SwitchParameter<>("InternalArrayish"+i.token(), Arrayish.Factory.class, ARRAYISH_FACTORIES.get(0), ARRAYISH_FACTORIES));
		}
	}
	@Override
	public StorageFactory asFactory(final Configuration config) {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				if (storage.steadyStateCapacity() == 0)
					return new EmptyConcreteStorage(storage);
				Configuration.SwitchParameter<Arrayish.Factory> factoryParam = config.getParameter("InternalArrayish"+storage.id(), Configuration.SwitchParameter.class, Arrayish.Factory.class);
				Arrayish.Factory factory = storage.type().isPrimitive() ? factoryParam.getValue() : Arrayish.ArrayArrayish.factory();
				return new InternalArrayConcreteStorage(factory.make(storage.type(), storage.steadyStateCapacity()), storage);
			}
		};
	}
}
