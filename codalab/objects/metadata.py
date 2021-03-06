'''
Metadata is a wrapper around all of the metadata rows for a single bundle.
Its constructor takes both the metadata and the bundle's metadata specs,
and validates the metadata before returning.
'''
from codalab.common import UsageError


class Metadata(object):
    def __init__(self, metadata_specs, metadata_dict):
        if isinstance(metadata_dict, (list, tuple)):
            metadata_dict = self.collapse_dicts(metadata_specs, metadata_dict)
        self._metadata_keys = set()
        for (key, value) in metadata_dict.iteritems():
            self.set_metadata_key(key, value)

    def validate(self, metadata_specs):
        '''
        Check that this metadata has the correct metadata keys and that it has
        metadata values of the correct types.
        '''
        expected_keys = set(spec.key for spec in metadata_specs)
        for key in self._metadata_keys:
            if key not in expected_keys:
                raise UsageError('Unexpected metadata key: %s' % (key,))
        for spec in metadata_specs:
            if spec.key in self._metadata_keys:
                value = getattr(self, spec.key)
                if not isinstance(value, spec.type):
                    raise UsageError(
                      'Metadata value for %s should be of type %s, was %s' %
                      (spec.key, spec.type, type(value))
                    )
            elif not spec.generated:
                raise UsageError('Missing metadata key: %s' % (spec.key,))

    def set_metadata_key(self, key, value):
        '''
        Set this Metadata object's key to be the given value. Record the key.
        '''
        self._metadata_keys.add(key)
        if isinstance(value, (set, list, tuple)):
            value = set(value)
        setattr(self, key, value)

    @classmethod
    def collapse_dicts(cls, metadata_specs, rows):
        '''
        Convert a list of Metadata dictionaries into a normalized metadata dict.
        '''
        metadata_dict = {}
        metadata_spec_dict = {}
        for spec in metadata_specs:
            if spec.type == set or not spec.generated:
                metadata_dict[spec.key] = spec.get_constructor()()
            metadata_spec_dict[spec.key] = spec
        for row in rows:
            (maybe_unicode_key, value) = (row['metadata_key'], row['metadata_value'])
            # If the key is Unicode text (which is the case if it was extracted from a
            # database), cast it to a string. This operation encodes it with UTF-8.
            key = str(maybe_unicode_key)
            spec = metadata_spec_dict[key]
            if spec.type == set:
                metadata_dict[key].add(value)
            else:
                if metadata_dict.get(key):
                    raise UsageError(
                      'Got duplicate values %s and %s for key %s' %
                      (metadata_dict[key], value, key)
                    )
                metadata_dict[key] = spec.get_constructor()(value)
        return metadata_dict

    def to_dicts(self, metadata_specs):
        '''
        Serialize this metadata object and return a list of dicts that can be saved
        to a MySQL table. These dicts should have the following keys:
          metadata_key
          metadata_value
        '''
        result = []
        for spec in metadata_specs:
            if spec.key in self._metadata_keys:
                value = getattr(self, spec.key)
                values = value if spec.type == set else (value,)
                for value in values:
                    result.append({
                      'metadata_key': unicode(spec.key),
                      'metadata_value': unicode(value),
                    })
        return result

    def to_dict(self):
        '''
        Serialize this metadata to human-readable JSON format. This format is NOT
        an appropriate one to save to a database.
        '''
        items = [(key, getattr(self, key)) for key in self._metadata_keys]
        return {
          key: list(value) if isinstance(value, (list, set, tuple)) else value
          for (key, value) in items
        }
