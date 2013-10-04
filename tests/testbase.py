import logging
import os
import sys

logging.basicConfig()

#Add LIBRARY_ROOT to python path for imports
LIBRARY_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.insert(0, LIBRARY_ROOT)
