#!/usr/bin/env python3

import stationConfig
import json, os, math, copy
from datetime import datetime, timedelta, date

class DataCombineAndCalc:
    def __init__(self):
        self.dataDir = stationConfig.dataDir
        self.stations = stationConfig.stations
        self.iterateStations()

    def iterateStations(self):
        for self.stn in self.stations:
            self.modelDict = self.ingestModelDict()
            self.obDict = self.ingestObDict()
            self.combinedDict = self.combineDicts()
            self.fhrBias = self.parseBiasData()
            self.genericWrite()

    def ingestModelDict(self):
        with open(self.stations[self.stn]["model"]) as modelJson:
            return json.load(modelJson)

    def ingestObDict(self):
        with open(self.stations[self.stn]["ob"]) as obJson:
            return json.load(obJson)

    def combineDicts(self):
        combinedDict = copy.deepcopy(self.modelDict)
        for eachCyc in list(self.modelDict.keys()):
            for eachFhr in list(self.modelDict[eachCyc].keys()):
                for eachParm in list(self.modelDict[eachCyc][eachFhr].keys()):
                    if self.modelDict[eachCyc][eachFhr][eachParm]["date"] in list(self.obDict.keys()):
                        combinedDict[eachCyc][eachFhr][eachParm]["ob"] = self.obDict[self.modelDict[eachCyc][eachFhr][eachParm]["date"]][eachParm]
                        eachBias = combinedDict[eachCyc][eachFhr][eachParm]["model"] - self.obDict[self.modelDict[eachCyc][eachFhr][eachParm]["date"]][eachParm]
                        if math.isnan(eachBias):
                            eachBias = "null"
                        combinedDict[eachCyc][eachFhr][eachParm]["bias"] = eachBias
        return combinedDict

    def parseBiasData(self):
        fhrDict = {}
        for eachCyc in list(self.combinedDict.keys()):
            for eachFhr in list(self.combinedDict[eachCyc].keys()):
                if eachFhr not in fhrDict:
                    fhrDict[eachFhr] = {}
                for eachParm in list(self.combinedDict[eachCyc][eachFhr].keys()):
                    if eachParm not in fhrDict[eachFhr]:
                        fhrDict[eachFhr][eachParm] = []
                    if "bias" in self.combinedDict[eachCyc][eachFhr][eachParm]:
                        fhrDict[eachFhr][eachParm].append(self.combinedDict[eachCyc][eachFhr][eachParm]["bias"])
        return fhrDict

    def genericWrite(self):
        with open(self.stations[self.stn]["bias"],"w") as outfile:
            json.dump(self.fhrBias,outfile)
        return


DataCombineAndCalc()