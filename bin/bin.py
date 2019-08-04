import sys
import os
BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(BASE_DIR)

from core.main import main


print(sys.argv)
main()
