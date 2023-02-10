#!/usr/bin/env python3

import stationConfig
import json, os, math, copy, sys
from datetime import datetime, timedelta, date

class DataCombineAndCalc:
    def __init__(self):
        self.dataDir = stationConfig.dataDir
        self.stations = stationConfig.stations
        self.iterateStations()

    def iterateStations(self):
        for self.stn in self.stations:
            print("\033[1;33m( {:s} ) - Comparing obs and model data for {:s}\033[0;0m".format(datetime.utcnow().strftime("%H:%M:%S"),self.stn))
            self.modelDict = self.ingestModelDict()
            self.obDict = self.ingestObDict()
            print("( {:s} ) - Creating a combined/aligned obs/model dictionary".format(datetime.utcnow().strftime("%H:%M:%S")))
            self.combinedDict = self.combineDicts()
            print("( {:s} ) - Writing dictionary to file".format(datetime.utcnow().strftime("%H:%M:%S")))
            self.genericWrite("paired",self.combinedDict,"datetime")
        sys.stdout.write("\033[1;32m( {:s} ) - Complete!\033[0;0m\n".format(datetime.utcnow().strftime("%H:%M:%S")))

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
                        if eachParm not in self.obDict[self.modelDict[eachCyc][eachFhr][eachParm]["date"]]:
                            combinedDict[eachCyc][eachFhr][eachParm]["ob"] = "null"
                            eachBias = "null"
                        elif eachParm != "q24" and (combinedDict[eachCyc][eachFhr][eachParm]["model"] == "null" or  self.obDict[self.modelDict[eachCyc][eachFhr][eachParm]["date"]][eachParm] == "null"):
                            combinedDict[eachCyc][eachFhr][eachParm]["ob"] = self.obDict[self.modelDict[eachCyc][eachFhr][eachParm]["date"]][eachParm]
                            eachBias = "null"
                        elif eachParm == "q24":
                            # QPF 24 is a beast of its own. Needs a custom method to sum the qpf06s
                            # If model data exists for this time
                            if combinedDict[eachCyc][eachFhr][eachParm]["model"] != "null":
                                qpfVal = self.calcQPF24(eachCyc,eachFhr,eachParm)
                                combinedDict[eachCyc][eachFhr][eachParm]["ob"] = qpfVal
                                # If the qpf24 came back as null then dont calc a bias.
                                if qpfVal != "null":
                                    eachBias = float(combinedDict[eachCyc][eachFhr][eachParm]["model"]) - qpfVal
                                else:
                                    eachBias = "null"
                            # And if no model data exists, then set everything to null anyway.
                            else:
                                combinedDict[eachCyc][eachFhr][eachParm]["ob"] = "null"
                                eachBias = "null"
                        else:
                            combinedDict[eachCyc][eachFhr][eachParm]["ob"] = self.obDict[self.modelDict[eachCyc][eachFhr][eachParm]["date"]][eachParm]
                            eachBias = combinedDict[eachCyc][eachFhr][eachParm]["model"] - self.obDict[self.modelDict[eachCyc][eachFhr][eachParm]["date"]][eachParm]
                        combinedDict[eachCyc][eachFhr][eachParm]["bias"] = eachBias
                    else:
                        combinedDict[eachCyc][eachFhr][eachParm]["ob"] = "null"
                        combinedDict[eachCyc][eachFhr][eachParm]["bias"] = "null"
        return combinedDict

    def calcQPF24(self,eachCyc,eachFhr,eachParm):
        q24Date = datetime.strptime(self.modelDict[eachCyc][eachFhr][eachParm]["date"],"%Y-%m-%d %H:%M:%S")
        qpf6Sum = 0.00
        for x in range(18, -6, -6):
            # Time of the first q06 ob we want to look for.
            q06Date = q24Date - timedelta(hours=x)
            q06DateStr = q06Date.strftime("%Y-%m-%d %H:%M:%S")
            try:
                q06Val = self.obDict[q06DateStr]["q06"]
                if q06Val != "null":
                    qpf6Sum += q06Val
            except:
                return "null"
        return qpf6Sum

    def genericWrite(self, fileKey, theDict, sortFmt):
        if sortFmt == "datetime":
            kysForSorting = [ datetime.strptime(x,"%Y-%m-%d %H:%M:%S") for x in list(theDict.keys()) ]
        elif sortFmt == "number":
            kysForSorting = [ int(x) for x in list(theDict.keys()) ]
        kysForSorting.sort()
        sorted_dict = {str(i):theDict[str(i)] for i in kysForSorting}
        with open(self.stations[self.stn][fileKey],"w") as outfile:
            fmtJsonStr = json.dumps(sorted_dict,indent=4,sort_keys=False)
            outfile.write(fmtJsonStr)
        return


DataCombineAndCalc()