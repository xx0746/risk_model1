import sys
import os
from os import path
from haversine import haversine
from geopy.distance import geodesic
import pandas as pd
import requests
import json
import math

sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
if sys.platform[0] == 'l':
    sys.path.append('/root/bdrisk/risk_project')
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
else:
    sys.path.append(r"D:\bdrisk-model\risk_model\risk_models")
    sys.path.append(path.dirname(path.dirname(path.dirname(os.getcwd()))))
    print(path.dirname(path.dirname(os.getcwd())))
from risk_models import *
from risk_models.TRACK.TR1.track_func import *


if __name__ == "__main__":
    a = [111.5, 36.08]
    b = [121.47, 31.23]
    a2 = [0, 0]
    b2 = [0, 0]
    a2[0], a2[1] = a[1], a[0]
    b2[0], b2[1] = b[1], b[0]
    distance = TrackDetection._geodistance(a, b)
    distance2 = haversine(a, b) * 1000
    distance3 = geodesic(b2, a2).km * 1000
    print(distance)
    print(distance2)
    print(distance3)
    print(distance2 - distance)
    print(distance3 - distance)