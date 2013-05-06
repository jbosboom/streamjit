package edu.mit.streamjit.impl.compiler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import edu.mit.streamjit.impl.compiler.insts.Instruction;
import edu.mit.streamjit.impl.compiler.insts.PhiInst;
import edu.mit.streamjit.impl.compiler.insts.TerminatorInst;
import edu.mit.streamjit.impl.compiler.types.VoidType;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Queue;
import java.util.Set;

/**
 * Eliminates dead code from methods or blocks.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/25/2013
 */
public final class DeadCodeElimination {
	private DeadCodeElimination() {}

	public static boolean eliminateDeadCode(Method method) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			changed |= makingProgress |= eliminateTriviallyDeadInsts(method);
			changed |= makingProgress |= eliminateUselessPhis(method);
		} while (makingProgress);
		return changed;
	}

	public static boolean eliminateDeadCode(BasicBlock block) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			changed |= makingProgress |= eliminateTriviallyDeadInsts(block);
			changed |= makingProgress |= eliminateUselessPhis(block);
		} while (makingProgress);
		return changed;
	}

	public static boolean eliminateTriviallyDeadInsts(Method method) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			for (BasicBlock block : method.basicBlocks())
				changed |= makingProgress |= eliminateTriviallyDeadInsts(block);
		} while (makingProgress);
		return changed;
	}

	public static boolean eliminateTriviallyDeadInsts(BasicBlock block) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			for (Instruction i : ImmutableList.copyOf(block.instructions()))
				if (!(i.getType() instanceof VoidType) && i.uses().isEmpty()) {
					i.eraseFromParent();
					changed = makingProgress = true;
				}
		} while (makingProgress);
		return changed;
	}

	public static boolean eliminateUselessPhis(Method method) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			for (BasicBlock block : method.basicBlocks())
				changed |= makingProgress |= eliminateUselessPhis(block);
		} while (makingProgress);
		return changed;
	}

	public static boolean eliminateUselessPhis(BasicBlock block) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			for (Instruction i : ImmutableList.copyOf(block.instructions())) {
				if (!(i instanceof PhiInst))
					continue;
				PhiInst pi = (PhiInst)i;

				if (Iterables.size(pi.incomingValues()) == 1) {
					pi.replaceInstWithValue(Iterables.getOnlyElement(pi.incomingValues()));
					makingProgress = true;
					continue;
				}

				ImmutableSet<Value> phiSources = phiSources(pi);
				if (phiSources.size() == 1) {
					pi.replaceInstWithValue(phiSources.iterator().next());
					makingProgress = true;
					continue;
				}
			}
			changed |= makingProgress;
		} while (makingProgress);
		return changed;
	}

	/**
	 * Finds all the non-phi values that might be the result of the given
	 * PhiInst.  This will look through intermediate PhiInsts in the hope that
	 * they all can only select one value.
	 * @param inst the phi instruction to find sources of
	 * @return a list of the non-phi values that might be the result
	 */
	private static ImmutableSet<Value> phiSources(PhiInst inst) {
		Queue<PhiInst> worklist = new ArrayDeque<>();
		Set<PhiInst> visited = Collections.newSetFromMap(new IdentityHashMap<PhiInst, Boolean>());
		ImmutableSet.Builder<Value> builder = ImmutableSet.builder();
		worklist.add(inst);
		visited.add(inst);

		while (!worklist.isEmpty()) {
			PhiInst pi = worklist.remove();
			for (Value v : pi.incomingValues())
				if (v instanceof PhiInst && !visited.contains((PhiInst)v)) {
					visited.add((PhiInst)v);
					worklist.add((PhiInst)v);
				} else if (!(v instanceof PhiInst))
					builder.add(v);
		}

		return builder.build();
	}
}
