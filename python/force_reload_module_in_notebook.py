import sys
import importlib

# Replace sample_import with package name
if sys.modules.get("sample_import"):
    _ = importlib.reload(sys.modules["sample_import"])

# Or with path to package
if sys.modules.get("src.sample_import"):
    _ = importlib.reload(sys.modules["src.sample_import"])