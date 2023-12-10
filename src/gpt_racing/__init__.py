import logging
from pathlib import Path

__version__ = "0.1.0"

logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent
SECRETS_PATH = ROOT.parent.parent / "secrets.json"
