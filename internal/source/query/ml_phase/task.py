from source.task_runner.tasks.score_assigning_task import ScoreAssigningTask
from source.utils.random_seed_provider import RandomSeedProvider


class ScoreAssigningTaskGenerator(object):
    @staticmethod
    def get_tasks(job_id, customer_id, quest_id, query_id, num_sub_kernels, sphere_id,
                  ml_conf, jobnik_session, query_execution_unit_seed, feature_flags, num_additional_tasks=0, is_past=True):
        """
        @type num_additional_tasks: C{int}
        @type feature_flags: C{dict}
        @type jobnik_session: object
        @type customer_id: C{str}
        @type job_id: C{str}
        @type quest_id: C{str}
        @type query_id: C{str}
        @type num_sub_kernels: C{int}
        @type sphere_id: C{str}
        @type ml_conf: C{dict}
        @type past_queries: C{tuple}
        @type is_past: C{bool}
        @rtype: C{list} of L{source.task_runner.tasks.score_assigning_task.ScoreAssigningTask}
        """
        return [
            ScoreAssigningTask(job_id, customer_id, quest_id, query_id, query_id, sphere_id, is_past,
                               ml_conf, sub_kernel_id, sub_kernel_id + 1, num_sub_kernels + num_additional_tasks,
                               RandomSeedProvider.generate_contextual_seed(query_execution_unit_seed, 'score_assigner',
                                                                           sub_kernel_id),
                               jobnik_session, feature_flags)
            for sub_kernel_id in xrange(num_sub_kernels)]
