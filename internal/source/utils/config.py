"""
This module implements a config parser for this app.
The config is in JSON format and will be represented as an object with internal objects

@author: Lior Regev
"""
import json

from types import TupleType, ListType
import jsonschema

COLLECTION_TYPES = (ListType, TupleType, set,)


class ConfigError(Exception):
    """
    Raised when there is an error with the configuration.
    """
    pass


class ConfigCollection(object):
    """
    This class represents a single config item which is a collection.
    """

    def __init__(self, collection=()):
        """
        @param collection: The collection from which to create the object.

        @return: None
        @rtype: C{NoneType}
        """
        self.__values = []
        for item in collection:
            self.append(item)

    def append(self, p_object):
        """
        @param p_object: The object to append.
        @type p_object: C{object}
        """
        if isinstance(p_object, COLLECTION_TYPES):
            self.__values.append(ConfigCollection(p_object))
        elif isinstance(p_object, dict):
            self.__values.append(ConfigItem.from_dict(p_object))
        else:
            self.__values.append(p_object)

    def to_builtin(self):
        """
        Converts the ConfigCollection object to a list.

        @return: A new list representing the ConfigCollection object
        @rtype: C{list}
        """
        result = []
        for item in self:
            if isinstance(item, (ConfigItem, ConfigCollection)):
                result.append(item.to_builtin())
            else:
                result.append(item)

        return result

    def __contains__(self, item):
        return item in self.__values

    def validate(self, rules, *args, **kwargs):
        """
        Validates the ConfigCollection object against the rules given.
        validation actually validates each item in the collection against the set of rules.

        @param rules: The rules to validate against, simple example::
            [ int ]
        A more complex example would be::
            [
                {
                    'name': (str, True),
                    'knows': ([ {'name': (str, True), 'age': (int, False)}, ], False)
                }
            ]
        @type rules: C{dict}

        @return: True if valid, False otherwise.
        @rtype: C{bool}
        """
        if len(rules) < len(self.__values):
            rules = list(rules)
            rules.extend([rules[-1]] * (len(self.__values) - len(rules)))

        try:
            for item, rule in zip(self, rules):
                simple_rule = type(rule) == type
                if simple_rule:
                    if not isinstance(item, rule):
                        raise ConfigError('item_idx={0}, rule={1}'.format(self.__values.index(item), rule))
                else:
                    if not item.validate(rule):
                        raise ConfigError('item_idx={0}, rule={1}'.format(self.__values.index(item), rule))
            return True
        except AttributeError:
            raise ConfigError()

    def __getitem__(self, item):
        return self.__values[item]

    def __getattr__(self, name):
        # Attribute lookups are delegated to the underlying file
        # and cached for non-numeric results
        # (i.e. methods are cached, closed and friends are not)
        a = getattr(self.__dict__, name)
        return a

    def __iter__(self):
        return iter(self.__values)

    def __len__(self):
        return len(self.__values)


class ConfigItem(object):
    """
    This class represents a single configuration item.
    """

    def __setattr__(self, key, value):
        """
        Overrides the default setattr to allow recursive parsing.

        It is written in a type-safe manner because there is no better way.
        """
        if isinstance(value, COLLECTION_TYPES):
            super(ConfigItem, self).__setattr__(key, ConfigCollection(value))
        elif isinstance(value, dict):
            super(ConfigItem, self).__setattr__(key, ConfigItem.from_dict(value))
        else:
            super(ConfigItem, self).__setattr__(key, value)

    @classmethod
    def from_dict(cls, input_dict):
        """
        This class method loads a new ConfigItem from the given dictionary
        @param input_dict: The input dictionary,
        @type input_dict: C{dict}

        @return: A new instance of ConfigItem
        @rtype: L{ConfigItem}
        """
        new_item = cls()
        for key, value in input_dict.iteritems():
            setattr(new_item, key, value)

        return new_item

    def to_builtin(self):
        """
        Converts the ConfigItem instance to a dictionary.
        @return: A dictionary representing the ConfigItem.
        @rtype: C{dict}
        """
        result = {key: val.to_builtin() if isinstance(val, (ConfigItem, ConfigCollection,)) else val
                  for key, val in self.__dict__.iteritems()}
        return result

    def validate(self, rules):
        """
        Validates the ConfigCollection object against the rules given.
        validation actually validates each item in the collection against the set of rules.

        @param rules: The rules to validate against. Each rule is comprised of a tuple
        of three items (rule, required).
        simple example::
            {
                'name': (str, True),
                'knows': ([ {'name': (str, True), 'age': (int, True)}, ], False)
            }
        @type rules: C{dict}

        @return: True if valid, False otherwise.
        @rtype: C{bool}
        """
        try:
            for field_name, rule_data in rules.iteritems():
                try:
                    rule, required, default = rule_data
                except ValueError:
                    # No default given, using None
                    rule, required, default = rule_data + (None,)
                try:
                    field = getattr(self, field_name)

                    if type(rule) == type:
                        valid = isinstance(field, rule)
                    else:
                        valid = field.validate(rule)

                    if not valid:
                        raise ConfigError('field_name={0}, rule={1}'.format(field_name, rule))
                except AttributeError:
                    if required:
                        raise ConfigError('field_name={0}, rule={1}'.format(field_name, rule))
                    else:
                        setattr(self, field_name, default)
            return True
        except ValueError:
            raise ConfigError()

    def __getattr__(self, name):
        # Attribute lookups are delegated to the underlying file
        # and cached for non-numeric results
        # (i.e. methods are cached, closed and friends are not)
        try:
            a = getattr(self.__dict__, name)
        except AttributeError:
            raise AttributeError('The configuration object does not contains the request field: {}'.format(name))
        return a

    def __iter__(self):
        return iter(self.__dict__)

    def __getitem__(self, item):
        return self.__dict__[item]

    def __setitem__(self, key, value):
        self.__setattr__(key, value)

    def __len__(self):
        return len(self.__dict__)


class ConfigParser(ConfigItem):
    """
    This class parses a config from a JSON string.
    """

    @classmethod
    def from_file(cls, file_path):
        """
        This class method loads a new ConfigItem from the given JSON file.
        @param file_path: The path JSON file.
        @type file_path: C{str}
        """
        with open(file_path) as input_file:
            config_dict = json.loads(input_file.read())
        return cls.from_dict(config_dict)

    @classmethod
    def from_json(cls, input_json):
        """
        This class method loads a new ConfigItem from the given JSON string.
        @param input_json: The input JSON string.
        @type input_json: C{str}
        """
        config_dict = json.loads(input_json)
        return cls.from_dict(config_dict)

    def to_json(self):
        """
        Returns a JSON representation of the current config

        @rtype: C{str}
        """
        return json.dumps(self.to_builtin(), allow_nan=False)

    def to_dict(self):
        return json.loads(self.to_json())

    def validate_schema(self, json_schema):
        """
        Validates the config again the given json_schema
        @param json_schema: the schema to validate
        """
        config_dict = self.to_dict()
        jsonschema.validate(config_dict, json_schema)

    def __repr__(self):
        return repr(self.to_builtin())
