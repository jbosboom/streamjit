package edu.mit.streamjit.impl.distributed.runtime.slave;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import static com.google.common.base.Preconditions.*;

import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.MessageConstraint;
import edu.mit.streamjit.impl.common.Configuration.IntParameter;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter;
import edu.mit.streamjit.impl.common.Configuration.PartitionParameter.BlobSpecifier;
import edu.mit.streamjit.impl.concurrent.SingleThreadedBlob;
import edu.mit.streamjit.impl.distributed.runtime.api.JsonStringProcessor;
import edu.mit.streamjit.impl.distributed.runtime.common.GlobalConstants;
import edu.mit.streamjit.util.json.Jsonifiers;

/**
 * @author Sumanan sumanan@mit.edu
 * @since May 27, 2013
 */
public class SlaveJsonStringProcessor implements JsonStringProcessor {

	Slave slave;

	public SlaveJsonStringProcessor(Slave slave) {
		this.slave = slave;
	}

	@Override
	public void process(String json) {
		ImmutableSet<Blob> blobSet = getBlobs(json);
		// TODO: Need to call the blob runner.
	}

	private ImmutableSet<Blob> getBlobs(String json) {
		Configuration cfg = Jsonifiers.fromJson(json, Configuration.class);

		IntParameter intparam = cfg.getParameter("machineID", IntParameter.class);
		if (intparam == null)
			throw new IllegalArgumentException("machineID is not available in the received configuraion");

		PartitionParameter partParam = cfg.getParameter("partition", PartitionParameter.class);
		if (partParam == null)
			throw new IllegalArgumentException("Partition parameter is not available in the received configuraion");

		String outterClass = (String) cfg.getExtraData(GlobalConstants.outterClassName);
		String topLevelWorkerName = (String) cfg.getExtraData(GlobalConstants.topLevelWorkerName);
		String jarFilePath = (String) cfg.getExtraData(GlobalConstants.jarFilePath);

		Worker<?, ?> topLevelWorker = getToplevelWorker(jarFilePath, outterClass, topLevelWorkerName);

		slave.setMachineID(intparam.getValue());
		List<BlobSpecifier> blobList = partParam.getBlobsOnMachine(slave.getMachineID());

		ImmutableSet.Builder<Blob> blobSet = ImmutableSet.builder();

		for (BlobSpecifier bs : blobList) {
			Set<Integer> workIdentifiers = bs.getWorkerIdentifiers();
			System.out.println(workIdentifiers.toString());
			ImmutableSet<Worker<?, ?>> workerset = bs.getWorkers(topLevelWorker);
			Blob b = new SingleThreadedBlob(workerset, Collections.<MessageConstraint> emptyList());
			blobSet.add(b);
		}

		return blobSet.build();
	}

	private Worker<?, ?> getToplevelWorker(String jarFilePath, String outterClass, String topStreamClass) {

		checkNotNull(jarFilePath);
		checkNotNull(topStreamClass);

		File jarFile = new java.io.File(jarFilePath);
		if (!jarFile.exists()) {
			System.out.println("Jar file not found....");
			System.exit(0);
		}

		URL url;
		try {
			url = jarFile.toURI().toURL();
			URL[] urls = new URL[] { url };

			ClassLoader loader = new URLClassLoader(urls);
			Class<?> clazz1 = loader.loadClass(outterClass);
			Class<?> innterClass = getInngerClass(clazz1, topStreamClass);
			System.out.println(innterClass.getSimpleName());
			return (Worker<?, ?>) innterClass.newInstance();

		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.out.println("Couldn't find the toplevel worker...Exiting");
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Couldn't find the toplevel worker...Exiting");
			System.exit(0);
		}
		return null;
	}

	private static Class<?> getInngerClass(Class<?> OutterClass, String InnterClassName) throws ClassNotFoundException {
		Class<?>[] kl = OutterClass.getClasses();
		for (Class<?> k : kl) {
			if (InnterClassName.equals(k.getSimpleName())) {
				return k;
			}
		}
		throw new ClassNotFoundException(String.format("Innter class %s is not found in the outter class %s", InnterClassName,
				OutterClass.getName()));
	}

}
