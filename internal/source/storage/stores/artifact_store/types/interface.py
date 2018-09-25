import uuid


def get_all_bases(cls):
    """
    @type cls: C{type}
    @rtype: C{list} of C{type}
    """
    result = []
    for base in cls.__bases__:
        if base == object:
            continue
        result.append(base)
        result.extend(get_all_bases(base))
    return result


class ArtifactInterface(object):
    type = 'artifact'
    operation = 'operation'

    def __init__(self, customer, execution_id):
        """
        @type customer: C{str}
        @type execution_id: C{str}
        """
        self.__execution_id = execution_id
        self.__customer = customer
        self.__message_id = uuid.uuid4().get_hex()

    @property
    def message_id(self):
        return self.__message_id

    def _to_dict(self):
        """
        Override this for every sub-class to allow dictionary representation.
        @rtype: C{dict}
        """
        return {
            'customer': self.__customer,
            'execution_id': self.__execution_id,
        }

    def to_dict(self):
        """
        Returns an dictionary representation of this artifact.

        **Note: Do no override this, rewrite _to_dict instead.
        @rtype: C{dict}
        """
        bases = get_all_bases(self.__class__)
        bases.reverse()
        result_dict = {}
        for base in bases:
            # noinspection PyUnresolvedReferences,PyProtectedMember
            result_dict.update(base._to_dict(self))

        result_dict.update(self._to_dict())
        result_dict['operation'] = self.operation
        return result_dict

    @classmethod
    def from_dict(cls, data):
        """
        Creates an instance from the given dictionary
        @type data: C{dict}
        @return: The new instance
        @rtype: L{ArtifactInterface}
        """
        # This hack right here finds out the names of every argument on the __init__ of the artifact
        args = set(cls.__init__.im_func.func_code.co_varnames[1:cls.__init__.im_func.func_code.co_argcount])
        # And this trick will filter the input dictionary based on acceptable arguments
        partial_dict = {key: value for key, value in data.iteritems() if len({key, 'kwargs'}.intersection(args))}
        return cls(**partial_dict)

    @property
    def customer(self):
        return self.__customer

    @property
    def execution_id(self):
        return self.__execution_id


class ExternalJobArtifactInterface(ArtifactInterface):
    type = 'job'

    def __init__(self, customer, execution_id, job_id, num_tasks):
        """
        @type customer: C{str}
        @type execution_id: C{str}
        @type job_id: C{str}
        @type num_tasks: C{int}
        """
        super(ExternalJobArtifactInterface, self).__init__(customer, execution_id)
        self.__num_tasks = num_tasks
        self.__job_id = job_id

    def _to_dict(self):
        return {
            'job_id': self.__job_id,
            'num_tasks': self.__num_tasks
        }

    @property
    def job_id(self):
        return self.__job_id

    @property
    def num_tasks(self):
        return self.__num_tasks
