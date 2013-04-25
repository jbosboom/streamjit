package edu.mit.streamjit.impl.compiler;

import com.google.common.base.Functions;
import static com.google.common.base.Preconditions.*;
import edu.mit.streamjit.impl.compiler.insts.Instruction;
import java.util.Map;

/**
 * Modeled on LLVM's so see theirs for documentation, at least for now.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 4/25/2013
 */
public final class Cloning {
	private Cloning() {}

	public static BasicBlock cloneBasicBlock(BasicBlock source, Map<Value, Value> vmap) {
		BasicBlock dest = new BasicBlock(source.getType().getModule(), source.getName()+"_clone");

		//Loop over all instructions, copying them over without remapping,
		//recording the mapping in the value map.
		for (Instruction i : source.instructions()) {
			Instruction ic = i.clone(Functions.<Value>identity());
			dest.instructions().add(ic);
			vmap.put(i, ic);
		}

		return dest;
	}

	public static void cloneMethod(Method source, Method dest, Map<Value, Value> vmap) {
		checkArgument(source.isResolved());
		checkArgument(dest.isResolved());
		checkArgument(dest.basicBlocks().isEmpty());
		for (Argument a : source.arguments())
			checkArgument(vmap.containsKey(a));

		for (BasicBlock oldBlock : source.basicBlocks()) {
			BasicBlock newBlock = cloneBasicBlock(oldBlock, vmap);
			dest.basicBlocks().add(newBlock);
			vmap.put(oldBlock, newBlock);
		}

		for (BasicBlock newBlock : dest.basicBlocks())
			for (Instruction newInst : newBlock.instructions())
				for (int i = 0; i < newInst.getNumOperands(); ++i)
					if (vmap.containsKey(newInst.getOperand(i)))
						newInst.setOperand(i, vmap.get(newInst.getOperand(i)));
	}
}
