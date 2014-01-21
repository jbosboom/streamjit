from opentuner.search import technique
from sjparameters import sjSwitchParameter

# Looks for sjSwitchParameters whose name starts with a given prefix and forces
# their value to true.  (TODO: make boolean sjSwitchParameters extend BooleanParameter instead)
class ForceTrue(technique.SearchTechnique):
	def __init__(self, prefix, *pargs, **kwargs):
		super(ForceTrue, self).__init__(*pargs, **kwargs)
		self.prefix = prefix

	def desired_configuration(self):
		for result in self.driver.results_query(objective_ordered = True):
			truecfg = self.force(result.configuration)
			if self.driver.has_results(truecfg):
				continue
			return truecfg
		return None

	def force(self, cfg):
		new_data = self.manipulator.copy(cfg.data)
		for param in self.manipulator.parameters(cfg.data):
			if isinstance(param, sjSwitchParameter) and param.name.startswith(self.prefix):
				param._set(new_data, 1) # corresponds to true
		return self.driver.get_configuration(new_data)

class ForceRemove(ForceTrue):
	def __init__(self, *pargs, **kwargs):
		super(ForceRemove, self).__init__("remove", *pargs, **kwargs)

class ForceFuse(ForceTrue):
	def __init__(self, *pargs, **kwargs):
		super(ForceFuse, self).__init__("fuse", *pargs, **kwargs)

class ForceUnbox(ForceTrue):
	def __init__(self, *pargs, **kwargs):
		super(ForceUnbox, self).__init__("unbox", *pargs, **kwargs)

# The default bandit, plus our custom techniques.
from opentuner.search import bandittechniques, differentialevolution, evolutionarytechniques, simplextechniques
technique.register(bandittechniques.AUCBanditMetaTechnique([
		differentialevolution.DifferentialEvolutionAlt(),
		evolutionarytechniques.UniformGreedyMutation(),
		evolutionarytechniques.NormalGreedyMutation(mutation_rate=0.3),
		simplextechniques.RandomNelderMead(),
		ForceRemove(),
		ForceFuse(),
		ForceUnbox(),
	], name = "StreamJITBandit"))
