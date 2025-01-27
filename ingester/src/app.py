import sys
import logging

from core.ingester import Ingester

logger = logging.getLogger("ingester")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [INGESTER - %(levelname)s] %(message)s")

fh = logging.FileHandler("/opt/spark/work-dir/log/ingester.log")
sh = logging.StreamHandler(sys.stdout)
fh.setFormatter(formatter)
sh.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(sh)

def main() -> None:
    logger.info("Initializing ingester...")
    Ingester().run()

if __name__ == "__main__":
    main()