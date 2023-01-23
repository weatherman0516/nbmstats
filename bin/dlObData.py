#!/usr/bin/env python3

import stationConfig
import re, json, urllib.request, os, gzip, math
from datetime import datetime, timedelta, date

class ObGetter:
    def __init__(self):
        self.dataDir = stationConfig.dataDir
        self.stations = stationConfig.stations
        self.iterateStations()

    def iterateStations(self):
        for self.stn in self.stations:
            self.modelDict = self.ingestModelDict()
            self.dataDates = self.compileDataDates()
            self.fileLookups()
            self.storeObs()
            self.writeData()
            # I think at this point we can come up with a script to run stats.

    def ingestModelDict(self):
        with open(self.stations[self.stn]["model"]) as stnJson:
            return json.load(stnJson)

    def compileDataDates(self):
        dataDatesDupes = []
        parmSeeker = "wsp" # Just give any parm key from the model json file so we can get the dates.
        for cycleKey in list(self.modelDict.keys()):
            for fhr in list(self.modelDict[cycleKey].keys()):
                dataDatesDupes.append(self.modelDict[cycleKey][fhr][parmSeeker]["date"])
        # Crazy simple way to remove duplicates
        dataDates = [*set(dataDatesDupes)]
        dataDates.sort()
        return dataDates

    # Attach year or something to temp stored file
    def fileLookups(self):
        now = datetime.utcnow()
        lCaseStn = self.stn.lower()
        for eachDateStr in self.dataDates:
            eachDate = datetime.strptime(eachDateStr,"%Y-%m-%d %H:%M:%S")
            monStr = eachDate.strftime("%b")
            monNum = eachDate.strftime("%-m")
            thisYear = eachDate.strftime("%Y")
            urlSuff = ".txt.gz"
            deltaDays = (now-eachDate).days
            if deltaDays <= 45:
                fileUrl = "https://www.ndbc.noaa.gov/data/realtime2/"+self.stn+".txt"
                #realtime
                try:
                    self.fileDL(fileUrl)
                except:
                    raise Exception("Date is within 45 days and a file for this station does not exist.")
            else:
                try:
                    urlPre = "https://www.ndbc.noaa.gov/data/stdmet/"
                    urlMid = monStr+"/"+lCaseStn+monNum+thisYear
                    fileUrl = urlPre+urlMid+urlSuff
                    # Mon/<lCasSid><monNum><yyyy>.txt.gz
                    #Recent
                    self.fileDL(fileUrl)
                except:
                    try:
                        urlPre = "https://www.ndbc.noaa.gov/data/historical/stdmet/"
                        urlMid = lCaseStn+"h"+thisYear
                        fileUrl = urlPre+urlMid+urlSuff
                        # <lCasSid>h<yyyy>.txt.gz
                        #Old
                        self.fileDL(fileUrl)
                    except:
                        raise Exception("Checked both recent and historical and cannot find a station file")
        return

    def fileDL(self,fileUrl):
        opener = urllib.request.build_opener()
        urllib.request.install_opener(opener)
        stnFile = self.stn + ".txt"
        destFile = os.path.join(self.dataDir,stnFile)
        if not os.path.isfile(destFile):
            urllib.request.urlretrieve(fileUrl,destFile)
            self.destFile = destFile

    def storeObs(self):
        with open(self.destFile) as f:
            obData = f.readlines()[2:]
        self.obDict = {}
        for obRow in obData:
            # Only grab top of the hour obs
            if int(obRow[14:16]) == 0:
                dateKey = datetime(int(obRow[:4]),int(obRow[5:7]),int(obRow[8:10]),int(obRow[11:13]),int(obRow[14:16]))
                # If the ob is needed (i.e. do we have model data for this time.
                if str(dateKey) in self.dataDates:
                    spd = obRow[20:25].strip()
                    if spd == "MM":
                        spd = float("NaN")
                    else:
                        # m/s -> log wind adj -> kts
                        spd = self.logWindAdj(float(spd))
                    gst = obRow[25:30].strip()
                    if gst == "MM":
                        gst = float("NaN")
                    else:
                        # m/s -> log wind adj -> kts
                        gst = self.logWindAdj(float(gst))
                    self.obDict[str(dateKey)] = {"wsp":spd,"wgs":gst}
        os.remove(self.destFile)
        return

    def logWindAdj(self, spd):
        d = 0
        z1 = 10
        z2 = self.stations[self.stn]["elev"]
        z0 = 2.0e-4
        k = 0.40
        # adjWnd = (spd/k)*(math.log((z-d)/z0))
        adjWnd = spd*((math.log((z1-d)/z0))/(math.log((z2-d)/z0)))
        adjWndKts = adjWnd * 1.93384
        return adjWndKts

    def writeData(self):
        with open(self.stations[self.stn]["ob"],"w") as outfile:
            json.dump(self.obDict,outfile)
            outfile.write("\n")
        return

ObGetter()