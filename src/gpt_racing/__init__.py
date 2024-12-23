import logging
from pathlib import Path
import loggerado

__version__ = "0.1.0"

logger = logging.getLogger(__name__)
loggerado.configure_logger(logger, "INFO", ansi=True)

ROOT = Path(__file__).parent
SECRETS_PATH = ROOT.parent.parent / "secrets.json"
CACHE_PATH = ROOT.parent.parent / ".cache"
ELO_MMR_PATH = ROOT.parent.parent / "submodules" / "Elo-MMR" / "multi-skill"

ELO_MMR_CACHE_PATH = ELO_MMR_PATH / "../cache"
ELO_MMR_RESULT_PATH = ELO_MMR_PATH / "../data"
