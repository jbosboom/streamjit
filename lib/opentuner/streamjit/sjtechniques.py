import abc
from opentuner.search import technique
from sjparameters import sjSwitchParameter, sjCompositionParameter

# base class for techniques that mutate the best configuration
class BestBasedTechnique(technique.SearchTechnique):
	__metaclass__ = abc.ABCMeta

	def __init__(self, *pargs, **kwargs):
		super(BestBasedTechnique, self).__init__(*pargs, **kwargs)

	def desired_configuration(self):
		for result in self.driver.results_query(objective_ordered = True):
			data = self.manipulator.copy(result.configuration.data)
			next_data = self.mutate(self.manipulator.parameters(data), data)
			next_cfg = self.driver.get_configuration(next_data)
			if next_cfg.id is not None: # don't resubmit
				continue
			return next_cfg
		return None

	@abc.abstractmethod
	def mutate(self, params, data):
		pass

class ForceTrue(BestBasedTechnique):
	def __init__(self, prefix, *pargs, **kwargs):
		super(ForceTrue, self).__init__(*pargs, **kwargs)
		self.prefix = prefix

	def mutate(self, params, data):
		for param in params:
			if isinstance(param, sjSwitchParameter) and param.name.startswith(self.prefix):
				param._set(data, 1)
		return data

class ForceRemove(ForceTrue):
	def __init__(self, *pargs, **kwargs):
		super(ForceRemove, self).__init__("remove", *pargs, **kwargs)

class ForceFuse(ForceTrue):
	def __init__(self, *pargs, **kwargs):
		super(ForceFuse, self).__init__("fuse", *pargs, **kwargs)

class ForceUnbox(ForceTrue):
	def __init__(self, *pargs, **kwargs):
		super(ForceUnbox, self).__init__("unbox", *pargs, **kwargs)

class ForceEqualDivision(technique.SearchTechnique):
	def __init__(self):
		super(ForceEqualDivision, self).__init__()

	def desired_configuration(self):
		for cfg in self.driver.results_query(objective_ordered = True):
			new_data = self.manipulator.copy(cfg.configuration.data)
			for param in self.manipulator.parameters(new_data):
				if isinstance(param, sjCompositionParameter):
					param.equal_division(new_data)
			new_cfg = self.driver.get_configuration(new_data)
			if self.driver.has_results(new_cfg):
				continue
			return new_cfg
		return None
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
		ForceEqualDivision()
	], name = "StreamJITBandit"))
