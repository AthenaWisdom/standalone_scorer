from source.task_queues.publishers.sqs_publisher import SQSTaskQueuePublisher
from source.task_runner.submitter_interface import MultipleSubmittersTaskSubmitter, QueueBasedTaskSubmitter


class TaskSubmitterComponent(object):
    def __init__(self, env_config, feature_flags, io_handler):
        self._task_submitter = self.get_task_submitter(env_config, feature_flags)

    @staticmethod
    def get_task_submitter(env_config, feature_flags):
        task_submitter = MultipleSubmittersTaskSubmitter()
        if feature_flags.get('use_rc', False):
            task_publisher = SQSTaskQueuePublisher(env_config['environment_name'], feature_flags)
            base_submitter = QueueBasedTaskSubmitter(task_publisher)
            task_submitter.add_submitter(base_submitter)
        return task_submitter
