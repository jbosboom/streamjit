/**
 * @author Sumanan sumanan@mit.edu
 * @since Mar 11, 2013
 */
package edu.mit.streamjit.apps.roundrobinspilitterexample;

import edu.mit.streamjit.api.CompiledStream;
import edu.mit.streamjit.api.Filter;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.StreamCompiler;
import edu.mit.streamjit.impl.concurrent.ConcurrentStreamCompiler;
import edu.mit.streamjit.impl.interp.DebugStreamCompiler;

public class RoundRobinSplitterExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws InterruptedException{
		
		RoundRobinSplitterMain rbs = new RoundRobinSplitterMain();
		StreamCompiler sc = new DebugStreamCompiler();
		//StreamCompiler sc = new ConcurrentStreamCompiler(4);
		CompiledStream<Integer, Void> stream = sc.compile(rbs);
		Float output;

		for (int i = 0; i < 10000; ++i) {
			stream.offer(i);
			/*
			 * while ((output = stream.poll()) != null)
			 * System.out.println(output);
			 */
		}
		stream.drain();
		stream.awaitDraining();
	}

	public static class RoundRobinSplitterMain extends Pipeline<Integer, Void> {
		
		public RoundRobinSplitterMain(){
		
		Splitjoin<Integer, Integer> splitJoin1 = new Splitjoin<>(
				new RoundrobinSplitter<Integer>(),
				new RoundrobinJoiner<Integer>());
		for (int i = 0; i < 3; i++) {
			splitJoin1.add(new Multplier(i + 1));
		}
		Multplier mp = new Multplier(1);
		//splitJoin1.add(new Pipeline<>(new Multplier(1), new Multplier(2), new Multplier(3)));
		splitJoin1.add(mp, mp, mp);
		//splitJoin1.add(mp);
		add(splitJoin1);
		add(new IntPrinter());
		}		
	}
	
	private static class IntPrinter extends Filter<Integer, Void> {

		public IntPrinter() {
			super(1, 0);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void work() {
			System.out.println(pop());
		}
	}

	private static class Multplier extends Filter<Integer, Integer> {
		private int factor;

		public Multplier(int factor) {
			super(1, 1);
			this.factor = factor;
		}

		@Override
		public void work() {
			push(this.factor*pop());
		}
	}	
}
