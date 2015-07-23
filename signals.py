import numpy as np

from Data import inializeTickMemorySubscriptions
from ADR import getUniverse, getORD, getADRFX, getFutures

ADRs = getUniverse()
universe = []
universe += ADRs.tolist()
universe += [getORD(x) for x in ADRs]
universe += [getADRFX(x) for x in ADRs]
universe += [getFutures(x) for x in ADRs]
universe = np.unique(np.asarray(universe)).tolist()

inializeTickMemorySubscriptions(universe)
