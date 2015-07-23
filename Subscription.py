

memory = Memory()

    ADRs = getUniverse()
    universe = []
    universe += [getORD(x) for x in ADRs]
    universe += [getADRFX(x) for x in ADRs]
    universe += [getFutures(x) for x in ADRs]
    universe = np.unique(np.asarray(universe)).tolist()
