package edu.mit.streamjit.util.bytecode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.Invokable;
import edu.mit.streamjit.util.bytecode.insts.BinaryInst;
import edu.mit.streamjit.util.bytecode.insts.CallInst;
import edu.mit.streamjit.util.bytecode.insts.CastInst;
import edu.mit.streamjit.util.bytecode.insts.Instruction;
import edu.mit.streamjit.util.bytecode.insts.LoadInst;
import edu.mit.streamjit.util.bytecode.insts.PhiInst;
import edu.mit.streamjit.util.bytecode.types.PrimitiveType;
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
			for (BasicBlock block : method.basicBlocks())
				changed |= makingProgress |= eliminateDeadCode(block);
		} while (makingProgress);
		return changed;
	}

	public static boolean eliminateDeadCode(BasicBlock block) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			changed |= makingProgress |= eliminateUnusedSideEffectFreeInsts(block);
			changed |= makingProgress |= removeDeadCasts(block);
			changed |= makingProgress |= eliminateBoxUnbox(block);
			changed |= makingProgress |= removeUnusedKnownSideEffectFreeCalls(block);
			changed |= makingProgress |= eliminateUselessPhis(block);
		} while (makingProgress);
		return changed;
	}

	public static boolean eliminateUnusedSideEffectFreeInsts(Method method) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			for (BasicBlock block : method.basicBlocks())
				changed |= makingProgress |= eliminateUnusedSideEffectFreeInsts(block);
		} while (makingProgress);
		return changed;
	}

	public static boolean eliminateUnusedSideEffectFreeInsts(BasicBlock block) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			for (Instruction i : ImmutableList.copyOf(block.instructions())) {
				if (!i.uses().isEmpty()) continue;
				//TODO: ArrayLengthInst of non-null array
				//TODO: ArrayLoadInst from non-null array known in bounds
				//(perhaps because length and index are both constants)
				if (i instanceof BinaryInst) {
					BinaryInst bi = (BinaryInst)i;
					//Division and remainder can throw ArithmeticException.
					if ((bi.getOperation().equals(BinaryInst.Operation.DIV) ||
							bi.getOperation().equals(BinaryInst.Operation.REM)) &&
							bi.getType().isIntegral() &&
							!knownNotZero(bi.getOperand(1)))
						continue;
					i.eraseFromParent();
					changed = makingProgress = true;
				}
				//TODO: InstanceofInst
				if (i instanceof LoadInst) {
					LoadInst li = (LoadInst)i;
					if (li.getLocation() instanceof Field) {
						//TODO: LoadInst of field of known non-null object
						continue;
					} else
						assert li.getLocation() instanceof LocalVariable;
					i.eraseFromParent();
					changed = makingProgress = true;
				}
				//TODO: NewArrayInst with known nonnegative size(s)
				//TODO: StoreInst of a local variable with no following loads
				//(conservative approximation: no loads at all)
			}
		} while (makingProgress);
		return changed;
	}

	private static boolean knownNotZero(Value v) {
		assert v.getType() instanceof PrimitiveType : v;
		//Trivial implementation: non-zero constants.
		if (v instanceof Constant) {
			Object c = ((Constant<?>)v).getConstant();
			if (c instanceof Boolean)
				return (Boolean)c;
			assert c instanceof Number : c;
			Number n = (Number)c;
			//Any primitive Number to double is a widening conversion.
			return n.doubleValue() == 0;
		}
		return false;
	}

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
				//If the boxing call has no other uses, it will be removed by
				//removeUnusedKnownSideEffectFreeCalls.
				changed = makingProgress = true;
			}
		} while (makingProgress);
		return changed;
	}

	public static boolean removeDeadCasts(Method method) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			for (BasicBlock block : method.basicBlocks())
				changed |= makingProgress |= removeDeadCasts(block);
		} while (makingProgress);
		return changed;
	}

	/**
	 * Removes cast instructions that cast a value to its own type.
	 *
	 * TODO: is it safe to remove casts to a supertype (e.g., Float to Object)?
	 * @param block the block to remove casts in
	 * @return true iff changes were made
	 */
	public static boolean removeDeadCasts(BasicBlock block) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			for (Instruction i : ImmutableList.copyOf(block.instructions())) {
				if (!(i instanceof CastInst)) continue;
				CastInst cast = (CastInst)i;
				if (cast.getType().equals(cast.getOperand(0).getType())) {
					cast.replaceInstWithValue(cast.getOperand(0));
					changed = makingProgress = true;
				}
			}
		} while (makingProgress);
		return changed;
	}


	public static boolean removeUnusedKnownSideEffectFreeCalls(Method method) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			for (BasicBlock block : method.basicBlocks())
				changed |= makingProgress |= removeUnusedKnownSideEffectFreeCalls(block);
		} while (makingProgress);
		return changed;
	}

	private static final ImmutableSet<Invokable<?, ?>> KNOWN_SIDE_EFFECT_FREE = ImmutableSet.<Invokable<?, ?>>builder()
			.addAll(BOXING_METHODS)
			.build();
	public static boolean removeUnusedKnownSideEffectFreeCalls(BasicBlock block) {
		boolean changed = false, makingProgress;
		do {
			makingProgress = false;
			for (Instruction i : ImmutableList.copyOf(block.instructions())) {
				if (!(i instanceof CallInst)) continue;
				if (!i.uses().isEmpty()) continue;
				CallInst call = (CallInst)i;
				if (!KNOWN_SIDE_EFFECT_FREE.contains(call.getMethod().getBackingInvokable())) continue;
				call.eraseFromParent();
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
