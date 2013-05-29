/**
 * @author Sumanan sumanan@mit.edu
 * @since May 10, 2013
 */
package edu.mit.streamjit.impl.distributed.runtime.master;

import java.io.IOException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.apps.minimal.Minimal.MinimalKernel;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.distributed.DistributedStreamCompiler;

public class Master {

	OneToOneElement<?, ?> topOneToOneElement;
	int noOfSlaves;

	Master(String jarFilePath, String outterClass, String topLevelClass, int noOfSlaves) {

		if (noOfSlaves < 0)
			throw new IllegalArgumentException("noOfSlaves can not be negative");

		Worker<?, ?> topWorker = getToplevelWorker(jarFilePath, outterClass, topLevelClass);

		if (!(topWorker instanceof OneToOneElement<?, ?>))
			throw new IllegalArgumentException("topLevelClass is not OneToOneElement<?,?>. Please pass an OneToOneElement<?,?>");

		this.topOneToOneElement = (OneToOneElement<?, ?>) topWorker;
		this.noOfSlaves = noOfSlaves;
	}

	public void run() throws InterruptedException {
		
		StreamCompiler sc = new DistributedStreamCompiler(2);
		CompiledStream<?, ?> stream = sc.compile(this.topOneToOneElement);
		Integer output;
		for (int i = 0; i < 1000000; ++i) {
			stream.offer();
		}
		stream.drain();
		System.out.println("Drain called");
		stream.awaitDraining();		
	}

	private Worker<?, ?> getToplevelWorker(String jarFilePath, String outterClass, String topStreamClass) {
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length < 4) {
			System.out.println(args.length);
			System.out.println("Not enough parameters passed. Please provide thr following parameters.");
			System.out.println("1: StreamJit application's jar file path");
			System.out.println("2: StreamJit application top level class name");
			System.out.println("3: StreamJit application Outter level class name");
			System.out.println("3: No of slaves");
		}

		String jarFilePath = args[0];
		String topLevelClass = args[1];
		String outterClass = args[2];
		int noOfSlaves = new Integer(args[3]);

		new Master(jarFilePath, outterClass, topLevelClass, noOfSlaves);
	}
}
