package edu.mit.streamjit.impl.compiler2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.Blob;
import edu.mit.streamjit.impl.blob.Blob.Token;
import edu.mit.streamjit.impl.common.Workers;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An Actor encapsulating a Worker.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 10/24/2013
 */
public final class WorkerActor extends Actor {
	private final Worker<?, ?> worker;
	private final ActorArchetype archetype;
	public WorkerActor(Worker<?, ?> worker, ActorArchetype archetype) {
		super(archetype.inputType(), archetype.outputType());
		this.worker = worker;
		this.archetype = archetype;
	}

	@Override
	public int id() {
		return Workers.getIdentifier(worker());
	}

	public Worker<?, ?> worker() {
		return worker;
	}

	public ActorArchetype archetype() {
		return archetype;
	}

	/**
	 * Sets up Actor connections based on the worker's predecessor/successor
	 * relationships, creating TokenActors and Storages as required.  This
	 * method depends on all Storage objects initially being single-input,
	 * single-output, and all Tokens being single-input, single-output (which
	 * they should always be by their nature).
	 * @param workers an immutable map of Workers to their Actors; workers not
	 * in the map are not in this blob
	 * @param tokens a map of Tokens to their Actors, being constructed as we go
	 * @param storage a table of (upstream, downstream) Actor to the Storage
	 * that connects them, being constructed as we go
	 * @param inputTokenId an int-by-value containing the next input TokenActor id, to be
	 * incremented after use
	 * @param outputTokenId an int-by-value containing the next output TokenActor id, to be
	 * decremented after use
	 */
	public void connect(ImmutableMap<Worker<?, ?>, WorkerActor> workers,
			Map<Token, TokenActor> tokens,
			Table<Actor, Actor, Storage> storage,
			int[] inputTokenId, int[] outputTokenId) {
		List<? extends Worker<?, ?>> predecessors = Workers.getPredecessors(worker);
		if (predecessors.isEmpty()) {
			Blob.Token t = Blob.Token.createOverallInputToken(worker);
			TokenActor ta = new TokenActor(t, inputTokenId[0]++);
			tokens.put(t, ta);
			Storage s = new Storage(ta, this);
			inputs().add(s);
			ta.outputs().add(s);
			storage.put(ta, this, s);
		}
		for (Worker<?, ?> w : predecessors) {
			Actor pred = workers.get(w);
			if (pred == null) {
				Token t = new Blob.Token(w, worker());
				pred = new TokenActor(t, inputTokenId[0]++);
				tokens.put(t, (TokenActor)pred);
			}
			Storage s = storage.get(pred, this);
			if (s == null) {
				s = new Storage(pred, this);
				storage.put(pred, this, s);
			}
			inputs().add(s);
			if (pred instanceof TokenActor)
				pred.outputs().add(s);
		}

		List<? extends Worker<?, ?>> successors = Workers.getSuccessors(worker);
		if (successors.isEmpty()) {
			Blob.Token t = Blob.Token.createOverallOutputToken(worker);
			TokenActor ta = new TokenActor(t, outputTokenId[0]--);
			tokens.put(t, ta);
			Storage s = new Storage(this, ta);
			outputs().add(s);
			ta.inputs().add(s);
			storage.put(this, ta, s);
		}
		for (Worker<?, ?> w : successors) {
			Actor succ = workers.get(w);
			if (succ == null) {
				Token t = new Blob.Token(worker(), w);
				succ = new TokenActor(t, outputTokenId[0]--);
				tokens.put(t, (TokenActor)succ);
			}
			Storage s = storage.get(this, succ);
			if (s == null) {
				s = new Storage(this, succ);
				storage.put(this, succ, s);
			}
			outputs().add(s);
			if (succ instanceof TokenActor)
				succ.inputs().add(s);
		}

		MethodHandle identity = MethodHandles.identity(int.class);
		inputIndexFunctions().addAll(Collections.nCopies(inputs().size(), identity));
		outputIndexFunctions().addAll(Collections.nCopies(outputs().size(), identity));
	}

	@Override
	public int peek(int input) {
		return worker().getPeekRates().get(input).max();
	}

	@Override
	public int pop(int input) {
		return worker().getPopRates().get(input).max();
	}

	@Override
	public int push(int output) {
		return worker().getPushRates().get(output).max();
	}

	@Override
	public String toString() {
		return String.format("%s@%d[%s]", getClass().getSimpleName(), id(), worker);
	}
}
