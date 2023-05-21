import pkgutil
import importlib
import dialects

def get_dialects():
    return [
        name.split('*')[1]
        for finder, name, ispkg
        in pkgutil.iter_modules(dialects.__path__, dialects.__name__+'*')
    ]

def load_dialect(dialect_name):
    return importlib.import_module('dialects.' + dialect_name)