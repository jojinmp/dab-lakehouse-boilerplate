__config_version__ = 1

GLOBALS = {"serializer": "{{major}}.{{minor}}.{{patch}}"}

FILES = [
    {
        "path": "src/dab_boilerplate/__init__.py",
        "serializer": '__version__ = "{{major}}.{{minor}}.{{patch}}"',
    },
    {
        "path": "README.md",
        "serializer": "Current version: {{major}}.{{minor}}.{{patch}}",
    },
    {
        "path": "pyproject.toml",
        "serializer": 'version = "{{major}}.{{minor}}.{{patch}}"',
    },
]

VERSION = [
    {"name": "major", "type": "integer", "start_value": 0},
    {"name": "minor", "type": "integer", "start_value": 0},
    {"name": "patch", "type": "integer", "start_value": 0},
]

ACTIONS = {
    "increment_patch": {"type": "increment", "field": "patch"},
    "reset_patch": {
        "type": "conditional_reset",
        "field": "patch",
        "condition": {"field": "minor", "action": "increment"},
    },
    "reset_minor": {
        "type": "conditional_reset",
        "field": "minor",
        "condition": {"field": "major", "action": "increment"},
    },
}
