from source.task_runner.tasks.clusters_extaction import ClustersExtractionTask
from source.utils.random_seed_provider import RandomSeedProvider


class ClustersExtractionTaskGenerator(object):
    @staticmethod
    def get_tasks(job_id, customer, sphere_id, run_pairs, jobnik_session, feature_flags):
        """
        @type feature_flags: C{dict}
        @type job_id: C{str}
        @type customer: C{str}
        @type sphere_id: C{str}
        @type run_pairs: C{list} of L{RunPair}
        @type jobnik_session: L{JobnikSession}
        @type feature_flags: C{dict}
        @rtype: C{list} of L{source.task_runner.tasks.clusters_extraction.ClustersExtractionTask}
        """
        initial_seed = 123
        return [ClustersExtractionTask(job_id, customer, sphere_id, run_pair.params.connecting_field,
                                       run_pair.input_csv.name, i + 1, len(run_pairs) + 1, jobnik_session,
                                       RandomSeedProvider.generate_contextual_seed(initial_seed,
                                                                                   run_pair.input_csv.name, i),
                                       feature_flags=feature_flags)
                for i, run_pair in enumerate(run_pairs)]
