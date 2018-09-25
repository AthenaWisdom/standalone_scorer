import logging
import os
import signal

from bootstrap import TaskRunnerBootstrapper
from source.utils.configure_logging import configure_logger

configure_logger(logging.getLogger('endor'), extra={'replication_controller': True})
# Only relevant for single task container
environment_name = os.environ.get('ENVIRONMENT_NAME', None)
queue_name = os.environ.get('QUEUE_NAME', None)
is_writer = os.environ.get('IS_WRITER', 'false') == 'true'
all_handlers = os.environ.get('ALL_HANDLERS', 'false') == 'true'
tasks_to_run = os.environ.get('TASKS_TO_RUN', None)
redis_host = os.environ.get('TASKS_REDIS_HOST', None)
task_runner = TaskRunnerBootstrapper.create_prod_task_runner(redis_host, environment_name, queue_name,
                                                             is_writer, all_handlers)

if tasks_to_run is None:
    task_runner.start()
    signal.signal(signal.SIGINT, task_runner.stop)
    signal.signal(signal.SIGTERM, task_runner.stop)
    task_runner.wait_for_completion()
else:
    task_runner.run_for_k_tasks(int(tasks_to_run))
