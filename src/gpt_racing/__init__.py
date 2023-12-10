import logging
from pathlib import Path

__version__ = "0.1.0"

logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent
SECRETS_PATH = ROOT.parent.parent / "secrets.json"
ELO_MMR_PATH = ROOT.parent.parent / "submodules" / "Elo-MMR" / "multi-skill"

ELO_MMR_CACHE_PATH = ELO_MMR_PATH / "../cache"
ELO_MMR_RESULT_PATH = ELO_MMR_PATH / "../data"
