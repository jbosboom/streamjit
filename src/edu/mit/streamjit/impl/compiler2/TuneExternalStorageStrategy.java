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
public final class TuneExternalStorageStrategy implements StorageStrategy {
	private final ImmutableList<Arrayish.Factory> ARRAYISH_FACTORIES = ImmutableList.of(
			Arrayish.ArrayArrayish.factory(),
			Arrayish.NIOArrayish.factory(),
			Arrayish.UnsafeArrayish.factory()
	);
	@Override
	public void makeParameters(Set<Worker<?, ?>> workers, Configuration.Builder builder) {
		for (IOInfo i : IOInfo.allEdges(workers)) {
			builder.addParameter(new Configuration.SwitchParameter<>("ExternalArrayish"+i.token(), Arrayish.Factory.class, ARRAYISH_FACTORIES.get(0), ARRAYISH_FACTORIES));
			builder.addParameter(Configuration.SwitchParameter.create("UseDoubleBuffers"+i.token(), true));
		}
	}
	@Override
	public StorageFactory asFactory(final Configuration config) {
		return new StorageFactory() {
			@Override
			public ConcreteStorage make(Storage storage) {
				if (storage.steadyStateCapacity() == 0)
					return new EmptyConcreteStorage(storage);

				Configuration.SwitchParameter<Arrayish.Factory> factoryParam = config.getParameter("ExternalArrayish"+storage.id(), Configuration.SwitchParameter.class, Arrayish.Factory.class);
				Arrayish.Factory factory = storage.type().isPrimitive() ? factoryParam.getValue() : Arrayish.ArrayArrayish.factory();
				Configuration.SwitchParameter<Boolean> useDoubleBuffersParam = config.getParameter("UseDoubleBuffers"+storage.id(), Configuration.SwitchParameter.class, Boolean.class);
				if (useDoubleBuffersParam.getValue()
						&& storage.steadyStateCapacity() == 2*storage.throughput() //no leftover data
						&& storage.isFullyExternal() //no reads of writes before adjust
						)
					return new DoubleArrayConcreteStorage(factory, storage);
				return new CircularArrayConcreteStorage(factory.make(storage.type(), storage.steadyStateCapacity()), storage);
			}
		};
	}
}