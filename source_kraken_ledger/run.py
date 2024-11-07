import sys
import logging
from source_kraken_ledger import SourceKrakenLedger
from airbyte_cdk.entrypoint import launch

def run():
    # Configure logging to display debug messages
    logging.basicConfig(level=logging.DEBUG)
    source = SourceKrakenLedger()
    launch(source, sys.argv[1:])
