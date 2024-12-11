from dataclasses import dataclass


@dataclass
class Result:
    """General Result class mimicking dict and DataClass."""

    def __init__(self, **kwargs):
        """Initialize with dynamic attributes."""
        self.__dict__.update(kwargs)

    def __iter__(self):
        """Iterate."""
        return iter(self.__dict__.items())

    def __getitem__(self, key):
        """Get item."""
        return self.__dict__[key]

    def __len__(self):
        """Length."""
        return len(self.__dict__)

    def __contains__(self, item):
        """Contains."""
        return item in self.__dict__

    def __setitem__(self, key, value):
        """Set item."""
        self.__dict__[key] = value

    def __delitem__(self, key):
        """Delete item."""
        del self.__dict__[key]

    def get(self, key, default=None):
        """Get."""
        return self.__dict__.get(key, default)

    def items(self):
        """Items."""
        return self.__dict__.items()

    def keys(self):
        """Keys."""
        return self.__dict__.keys()

    def values(self):
        """Values."""
        return self.__dict__.values()
