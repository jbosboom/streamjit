package edu.mit.streamjit.impl.compiler;

import static com.google.common.base.Preconditions.*;
import com.google.common.collect.ImmutableMap;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jlinalg.AffineLinearSubspace;
import org.jlinalg.LinSysSolver;
import org.jlinalg.Matrix;
import org.jlinalg.Vector;
import org.jlinalg.rational.Rational;

/**
 * Computes the steady-state multiplicities of filters in a (unstructured)
 * graph.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 6/9/2013
 */
public final class Scheduler {
	private Scheduler() {}

	/**
	 * Represents one communication channel in the schedule.  The input thing
	 * (typically a Worker or StreamNode) produces at some fixed rate and the
	 * output thing consumes at some fixed rate.
	 * @param <T> the type of the things (typically Worker or StreamNode)
	 */
	public static class Channel<T> {
		public final T inputThing, outputThing;
		public final int inputRate, outputRate;
		/**
		 * The change in buffer size between these things after one execution of
		 * this schedule.  For steady-state scheduling, this should be 0; for
		 * initialization scheduling, this should be the desired buffering.
		 */
		public final int bufferDelta;
		public Channel(T inputThing, T outputThing, int inputRate, int outputRate) {
			this(inputThing, outputThing, inputRate, outputRate, 0);
		}
		public Channel(T inputThing, T outputThing, int inputRate, int outputRate, int bufferDelta) {
			this.inputThing = checkNotNull(inputThing);
			this.outputThing = checkNotNull(outputThing);
			this.inputRate = inputRate;
			this.outputRate = outputRate;
			this.bufferDelta = bufferDelta;
		}
	}

	/**
	 * Compute the steady-state multiplicities.
	 * @param <T> the type of thing
	 * @param channels the communication channels
	 * @return a map of things to multiplicities
	 */
	public static <T> ImmutableMap<T, Integer> schedule(List<Channel<T>> channels) {
		Map<T, Integer> thingIds = new HashMap<>();
		int things = 0;
		for (Channel<T> channel : channels) {
			if (!thingIds.containsKey(channel.inputThing))
				thingIds.put(channel.inputThing, things++);
			if (!thingIds.containsKey(channel.outputThing))
				thingIds.put(channel.outputThing, things++);
		}

		Rational[][] matrixArray = new Rational[channels.size()][things];
		Rational[] vectorArray = new Rational[channels.size()];
		for (int i = 0; i < channels.size(); ++i) {
			Arrays.fill(matrixArray[i], Rational.FACTORY.get(0));
			Channel<T> c = channels.get(i);
			matrixArray[i][thingIds.get(c.inputThing)] = Rational.FACTORY.get(c.inputRate);
			matrixArray[i][thingIds.get(c.outputThing)] = Rational.FACTORY.get(-c.outputRate);
			vectorArray[i] = Rational.FACTORY.get(c.bufferDelta);
		}
		Matrix<Rational> matrix = new Matrix<>(matrixArray);
		Vector<Rational> vector = new Vector<>(vectorArray);
		AffineLinearSubspace<Rational> solutionSpace = LinSysSolver.solutionSpace(matrix, vector).normalize();
		System.out.println(solutionSpace);
		if (vector.isZero())
			assert solutionSpace.getInhomogenousPart() == null;
		assert solutionSpace.getDimension() == 1;

		Vector<Rational> homogeneous = solutionSpace.getGeneratingSystem()[0];
		if (homogeneous.getEntry(1).compareTo(Rational.FACTORY.zero()) < 0)
			homogeneous = homogeneous.multiply(Rational.FACTORY.get(-1));
		Vector<Rational> v = solutionSpace.getGeneratingSystem()[0];
		if (solutionSpace.getInhomogenousPart() != null)
			v = v.add(solutionSpace.getInhomogenousPart());
		while (!validSchedule(v))
			v = v.add(homogeneous);
//		for (int i = 1; i <= v.length(); ++i) {
//			BigInteger d = v.getEntry(i).getDenominator();
//			if (!d.equals(BigInteger.ONE))
//				v.multiplyReplace(Rational.FACTORY.get(d));
//		}

		System.out.println(v);

		ImmutableMap.Builder<T, Integer> retval = ImmutableMap.<T, Integer>builder();
		for (Map.Entry<T, Integer> e : thingIds.entrySet()) {
			BigInteger mult = v.getEntry(e.getValue()+1).getNumerator();
			assert mult.signum() == 1;
			assert mult.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) < 0 : mult;
			retval.put(e.getKey(), v.getEntry(e.getValue()+1).getNumerator().intValue());
		}

		return retval.build();
	}

	private static boolean validSchedule(Vector<Rational> v) {
		for (int i = 1; i < v.length(); ++i)
			if (!v.getEntry(i).getDenominator().equals(BigInteger.ONE) || v.getEntry(i).getNumerator().signum() <= 0)
				return false;
		return true;
	}

	public static void main(String[] args) {
		Object a = new Object(), b = new Object(), c = new Object(), d = new Object();
		Channel<Object> channel1 = new Channel<>(a, b, 3, 2);
		Channel<Object> channel2 = new Channel<>(a, c, 5, 7, 2);
		Channel<Object> channel3 = new Channel<>(b, d, 3, 1);
		ImmutableMap<Object, Integer> schedule = schedule(Arrays.asList(channel1, channel2, channel3));
		System.out.println(schedule);
	}
}
