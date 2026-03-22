import logging
import os
from pathlib import Path
import loggerado

__version__ = "0.1.0"

logger = logging.getLogger(__name__)
loggerado.configure_logger(logger, "INFO", ansi=True)

ROOT = Path(__file__).parent
CACHE_PATH = Path(os.environ.get("CACHE_PATH", Path.cwd() / ".cache"))
