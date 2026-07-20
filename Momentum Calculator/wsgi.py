import sys
path = '/home/soumyamitra2010/Momentum_Calculator'
if path not in sys.path:
    sys.path.append(path)

from app import app as application