package org.mit.jstreamit;

/**
 *
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 11/7/2012
 */
public abstract class Filter<I, O> implements StreamElement<I, O> {
	private final Rate popRate, pushRate, peekRate;

	/**
	 * Creates a new Filter object with the given pop and push rates and a peek
	 * rate of 0.
	 * @param popRate the pop rate
	 * @param pushRate the push rate
	 */
	public Filter(int popRate, int pushRate) {
		//TODO: should we pass pushRate for peekRate instead of 0?
		this(popRate, pushRate, 0);
	}

	/**
	 * Creates a new Filter object with the given pop, push and peek rates.
	 * @param popRate the pop rate
	 * @param pushRate the push rate
	 * @param peekRate the peek rate
	 */
	public Filter(int popRate, int pushRate, int peekRate) {
		this(Rate.create(popRate), Rate.create(pushRate), Rate.create(peekRate));
	}

	/**
	 * Creates a new Filter object with the given pop, push and peek rates.
	 * @param popRate the pop rate
	 * @param pushRate the push rate
	 * @param peekRate the peek rate
	 */
	public Filter(Rate popRate, Rate pushRate, Rate peekRate) {
		if (popRate == null || pushRate == null || peekRate == null)
			throw new IllegalArgumentException(
					String.format("pop %s push %s peek %s",
					popRate, pushRate, peekRate));
		this.popRate = popRate;
		this.pushRate = pushRate;
		this.peekRate = peekRate;
	}

	public abstract void work();

	//These don't actually do anything, they're just for the JIT to pattern-match against.
	protected final I pop() {return null;};
	protected final I peek(int position) {return null;};
	protected final void push(O item) {};
}
