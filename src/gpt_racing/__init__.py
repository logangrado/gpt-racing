import logging
import os
from pathlib import Path
import loggerado

__version__ = "0.1.0"

logger = logging.getLogger(__name__)
loggerado.configure_logger(logger, "INFO", ansi=True)

ROOT = Path(__file__).parent
SECRETS_PATH = Path(os.environ.get("SECRETS_PATH", Path.cwd() / "secrets.json"))
CACHE_PATH = Path(os.environ.get("CACHE_PATH", Path.cwd() / ".cache"))
ELO_MMR_PATH = ROOT.parent.parent / "submodules" / "Elo-MMR" / "multi-skill"

ELO_MMR_CACHE_PATH = ELO_MMR_PATH / "../cache"
ELO_MMR_RESULT_PATH = ELO_MMR_PATH / "../data"
