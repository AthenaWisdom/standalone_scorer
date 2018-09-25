class TaskKey(object):
    def __init__(self, execution_id, task_ordinal, job_id):
        """
        @type job_id: C{str}
        @type execution_id: C{str}
        @type task_ordinal: C{int}
        """
        self.__job_id = job_id
        self.__task_ordinal = task_ordinal
        self.__execution_id = execution_id

    @property
    def task_ordinal(self):
        return self.__task_ordinal

    @property
    def execution_id(self):
        return self.__execution_id

    @property
    def job_id(self):
        return self.__job_id

    def __str__(self):
        return '{}__{}__{}'.format(self.__job_id, self.__execution_id, self.__task_ordinal)

    def __unicode__(self):
        return u'{}__{}__{}'.format(self.__job_id, self.__execution_id, self.__task_ordinal)
