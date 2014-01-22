package edu.mit.streamjit.tuner;

import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.common.Workers;
import edu.mit.streamjit.test.Benchmark;
import edu.mit.streamjit.test.Benchmarker;

/**
 * Prints out default configurations given a blob factory class and benchmark.
 * TODO: if the factory needs arguments, pass them in a Configuration object's
 * third parameter?  pass everything in extra data?
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 1/21/2014
 */
public final class ConfigGenerator2 {
	private ConfigGenerator2() {}

	public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		Class<? extends BlobFactory> factoryClass = Class.forName(args[0]).asSubclass(BlobFactory.class);
		BlobFactory factory = factoryClass.newInstance();
		Benchmark bm = Benchmarker.getBenchmarkByName(args[1]);
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		bm.instantiate().visit(cwv);
		Configuration config = factory.getDefaultConfiguration(Workers.getAllWorkersInGraph(cwv.getSource()));
		System.out.println(config.toJson());
	}
}
