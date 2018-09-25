from source.task_runner.tasks.score_assigning_task import ScoreAssigningTask


class ScoreAssignerExecutionEngineInterface(object):
    def run_score_assigner(self, tasks):
        """
        Runs the score assigner on the given tasks

        @param tasks: The tasks to run
        @type tasks: C{list} of L{ScoreAssigningTask}
        """
        raise NotImplementedError()
