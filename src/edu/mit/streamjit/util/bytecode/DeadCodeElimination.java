package edu.mit.streamjit.util.bytecode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.Invokable;
import edu.mit.streamjit.util.bytecode.insts.CallInst;
import edu.mit.streamjit.util.bytecode.insts.Instruction;
import edu.mit.streamjit.util.bytecode.insts.PhiInst;
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
//			changed |= makingProgress |= eliminateTriviallyDeadInsts(method);
			changed |= makingProgress |= eliminateBoxUnbox(method);
			changed |= makingProgress |= eliminateUselessPhis(method);
		} while (makingProgress);
		return changed;
	}

	public static boolean eliminateDeadCode(BasicBlock block) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
//			changed |= makingProgress |= eliminateTriviallyDeadInsts(block);
			changed |= makingProgress |= eliminateBoxUnbox(block);
			changed |= makingProgress |= eliminateUselessPhis(block);
		} while (makingProgress);
		return changed;
	}

	//TODO: these need a much more nuanced understanding of side effects to be safe.
//	public static boolean eliminateTriviallyDeadInsts(Method method) {
//		boolean changed = false, makingProgress;
//		do {
//			makingProgress = false;
//			for (BasicBlock block : method.basicBlocks())
//				changed |= makingProgress |= eliminateTriviallyDeadInsts(block);
//		} while (makingProgress);
//		return changed;
//	}
//
//	public static boolean eliminateTriviallyDeadInsts(BasicBlock block) {
//		boolean changed = false, makingProgress;
//		do {
//			makingProgress = false;
//			for (Instruction i : ImmutableList.copyOf(block.instructions()))
//				if (!(i.getType() instanceof VoidType) && i.uses().isEmpty()) {
//					i.eraseFromParent();
//					changed = makingProgress = true;
//				}
//		} while (makingProgress);
//		return changed;
//	}

	public static boolean eliminateBoxUnbox(Method method) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			for (BasicBlock block : method.basicBlocks())
				changed |= makingProgress |= eliminateBoxUnbox(block);
		} while (makingProgress);
		return changed;
	}

	private static final ImmutableList<Invokable<?, ?>> BOXING_METHODS;
	private static final ImmutableList<Invokable<?, ?>> UNBOXING_METHODS;
	static {
		ImmutableList.Builder<Invokable<?, ?>> boxingBuilder = ImmutableList.builder();
		ImmutableList.Builder<Invokable<?, ?>> unboxingBuilder = ImmutableList.builder();
		for (Class<?> w : Primitives.allWrapperTypes()) {
			if (w.equals(Void.class)) continue;
			Class<?> prim = Primitives.unwrap(w);
			try {
				boxingBuilder.add(Invokable.from(w.getMethod("valueOf", prim)));
				unboxingBuilder.add(Invokable.from(w.getMethod(prim.getName()+"Value")));
			} catch (NoSuchMethodException ex) {
				throw new AssertionError("Can't happen!", ex);
			}
		}
		BOXING_METHODS = boxingBuilder.build();
		UNBOXING_METHODS = unboxingBuilder.build();
	}

	/**
	 * Replaces the result of an unboxing operation whose source is a boxing
	 * operation with the value that was boxed, and also removes the boxing
	 * operation if it has no other uses.
	 * @param block the block to operate on (note that the boxing operation may
	 * be outside this block!)
	 * @return true iff changes were made
	 */
	public static boolean eliminateBoxUnbox(BasicBlock block) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			for (Instruction i : ImmutableList.copyOf(block.instructions())) {
				if (!(i instanceof CallInst)) continue;
				CallInst fooValue = (CallInst)i;
				int index = UNBOXING_METHODS.indexOf(fooValue.getMethod().getBackingInvokable());
				if (index == -1) continue;
				Value receiver = Iterables.getOnlyElement(fooValue.arguments());
				if (!(receiver instanceof CallInst)) continue;
				CallInst valueOf = (CallInst)receiver;
				if (!valueOf.getMethod().getBackingInvokable().equals(BOXING_METHODS.get(index))) continue;
				fooValue.replaceInstWithValue(valueOf.getArgument(0));
				if (valueOf.uses().isEmpty())
					valueOf.eraseFromParent();
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
