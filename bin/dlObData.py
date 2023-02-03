#!/usr/bin/env python3

import stationConfig
import re, json, urllib.request, os, gzip, math, sys
from datetime import datetime, timedelta, date

class ObGetter:
    def __init__(self):
        self.dataDir = stationConfig.dataDir
        self.stations = stationConfig.stations
        self.windAdjMethod = stationConfig.windAdjMethod
        self.iterateStations()

    def iterateStations(self):
        for self.stn in self.stations:
            print("\033[1;33m( {:s} ) - Working on obs data for {:s}\033[0;0m".format(datetime.utcnow().strftime("%H:%M:%S"),self.stn))
            self.destFiles = {}
            self.modelDict = self.ingestModelDict()
            self.dataDates = self.compileDataDates()
            self.obSource = self.stations[self.stn]["type"]
            if self.obSource == "marine":
                self.fileLookupsMarine()
                self.storeObsMarine()
                self.writeData()
            else:
                self.stationIdNum,self.wban,tmpFile = self.stnIdLookup()
                self.fileLookupsLand()
                self.storeObsLand()
                self.writeData()
        sys.stdout.write("\033[1;32m( {:s} ) - Complete!\033[0;0m\n".format(datetime.utcnow().strftime("%H:%M:%S")))
        os.remove(tmpFile)

            # I think at this point we can come up with a script to run stats.

    def ingestModelDict(self):
        with open(self.stations[self.stn]["model"]) as stnJson:
            return json.load(stnJson)

    def compileDataDates(self):
        print("( {:s} ) - Parsing dates available within model data file.".format(datetime.utcnow().strftime("%H:%M:%S")))
        dataDatesDupes = []
        parmSeeker = "wsp" # Just give any parm key from the model json file so we can get the dates.
        for cycleKey in list(self.modelDict.keys()):
            for fhr in list(self.modelDict[cycleKey].keys()):
                dataDatesDupes.append(self.modelDict[cycleKey][fhr][parmSeeker]["date"])
        # Crazy simple way to remove duplicates
        dataDates = [*set(dataDatesDupes)]
        dataDates.sort()
        ## We need to have at least 6 hr resolution through the extended for grabbing qpf06 obs from metar 6 group
        pendingDates = []
        for idx,eachDate in enumerate(dataDates):
            try:
                hrDelta = datetime.strptime(dataDates[idx+1],"%Y-%m-%d %H:%M:%S") - datetime.strptime(eachDate,"%Y-%m-%d %H:%M:%S")
                if hrDelta.total_seconds() == 43200:
                    dateToAdd = str(datetime.strptime(eachDate,"%Y-%m-%d %H:%M:%S") + timedelta(hours=6))
                    pendingDates.append(dateToAdd)
            except:
                continue
        dataDates += pendingDates
        dataDates.sort()
        return dataDates

    def stnIdLookup(self):
        print("( {:s} ) - Retrieving station id from NCEI".format(datetime.utcnow().strftime("%H:%M:%S")))
        lookupUrl = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt"
        opener = urllib.request.build_opener()
        urllib.request.install_opener(opener)
        lookupFile = "isd-history.txt"
        destFile = os.path.join(self.dataDir,lookupFile)
        if not os.path.isfile(destFile):
            urllib.request.urlretrieve(lookupUrl,destFile)
        with open(destFile) as stnIdFile:
            stnEntries = stnIdFile.readlines()
        for row in stnEntries:
            txtId = row[51:56].replace(" ","")
            if txtId == self.stn:
                stnNum = row[0:6].replace(" ","")
                wban = row[7:12].replace(" ","")
                if stnNum != '999999':
                    return stnNum,wban,destFile

    # Attach year or something to temp stored file
    def fileLookupsMarine(self):
        print("( {:s} ) - Determining which ob file to retrieve".format(datetime.utcnow().strftime("%H:%M:%S")))
        urlDict = {"primary":"https://www.ndbc.noaa.gov/data/realtime2/"+self.stn+".txt","secondary":"https://www.ndbc.noaa.gov/data/derived2/"+self.stn+".dmv"}
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
                for urlKey in list(urlDict.keys()):
                    fileExt = urlDict[urlKey][len(urlDict[urlKey])-3:]
                    try:
                        self.fileDL(fileUrl,urlKey,fileExt)
                    except:
                        raise Exception("Date is within 45 days and a file for this station does not exist.")
            else:
                try:
                    urlPre = "https://www.ndbc.noaa.gov/data/stdmet/"
                    urlMid = monStr+"/"+lCaseStn+monNum+thisYear
                    fileUrl = urlPre+urlMid+urlSuff
                    # Mon/<lCasSid><monNum><yyyy>.txt.gz
                    #Recent
                    self.fileDL(fileUrl,"txt")
                except:
                    try:
                        urlPre = "https://www.ndbc.noaa.gov/data/historical/stdmet/"
                        urlMid = lCaseStn+"h"+thisYear
                        fileUrl = urlPre+urlMid+urlSuff
                        # <lCasSid>h<yyyy>.txt.gz
                        #Old
                        self.fileDL(fileUrl,"txt")
                    except:
                        raise Exception("Checked both recent and historical and cannot find a station file")
        return

    def fileLookupsLand(self):
        urlPrefix = "https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/"
        allYears = [ datetime.strptime(yr,"%Y-%m-%d %H:%M:%S").year for yr in self.dataDates ]
        uniqueYears = [*set(allYears)]
        for eachYear in uniqueYears:
            fullUrl = urlPrefix + str(eachYear) + "/" + self.stationIdNum + "-" + self.wban + "-" + str(eachYear) + ".gz"
            self.fileDL(fullUrl,"primary","gz")

    def fileDL(self,fileUrl,urlKey,ext):
        print("( {:s} ) - Selected URL is: {:s}".format(datetime.utcnow().strftime("%H:%M:%S"),fileUrl))
        opener = urllib.request.build_opener()
        urllib.request.install_opener(opener)
        stnFile = self.stn + "." + ext
        destFile = os.path.join(self.dataDir,stnFile)
        if not os.path.isfile(destFile):
            urllib.request.urlretrieve(fileUrl,destFile)
            self.destFiles[urlKey] = destFile

    def storeObsMarine(self):
        print("( {:s} ) - Parsing obs file and storing to dictionary.".format(datetime.utcnow().strftime("%H:%M:%S")))
        with open(self.destFiles["primary"]) as fp:
            obDataPrimary = fp.readlines()[2:]
        with open(self.destFiles["secondary"]) as fs:
            obDataSecondary = fs.readlines()[2:]
        self.obDict = {}
        parmLocDict = {
            "ndbc":{"tmp":{"start":61,"stop":65,"urlKey":"primary"},"wsp":{"start":35,"stop":41,"urlKey":"secondary"},"wgs":{"start":42,"stop":48,"urlKey":"secondary"}},
            "equation":{"tmp":{"start":61,"stop":65,"urlKey":"primary"},"wsp":{"start":20,"stop":25,"urlKey":"primary"},"wgs":{"start":25,"stop":30,"urlKey":"primary"}}}
        for obIdx, obRowPrimary in enumerate(obDataPrimary):
            if int(obRowPrimary[14:16]) == 0:
                dateKey = datetime(int(obRowPrimary[:4]),int(obRowPrimary[5:7]),int(obRowPrimary[8:10]),int(obRowPrimary[11:13]),int(obRowPrimary[14:16]))
                if str(dateKey) in self.dataDates:
                    self.obDict[str(dateKey)] = {}
                    for eachParm in list(parmLocDict[self.windAdjMethod].keys()):
                        if parmLocDict[self.windAdjMethod][eachParm]["urlKey"] == "primary":
                            val = obRowPrimary[parmLocDict[self.windAdjMethod][eachParm]["start"]:parmLocDict[self.windAdjMethod][eachParm]["stop"]].strip()
                        else:
                            val = obDataSecondary[obIdx][parmLocDict[self.windAdjMethod][eachParm]["start"]:parmLocDict[self.windAdjMethod][eachParm]["stop"]].strip()
                        if val == "MM":
                            val = float("NaN")
                        else:
                            if eachParm in ["wsp","wgs"]:
                                val = self.logWindAdj(float(val))
                            elif eachParm in ["tmp"]:
                                val = round((float(val)*(9/5))+32)
                        self.obDict[str(dateKey)][eachParm] = val
        os.remove(self.destFiles["primary"])
        os.remove(self.destFiles["secondary"])

    def storeObsLand(self):
        print("( {:s} ) - Parsing obs file and storing to dictionary.".format(datetime.utcnow().strftime("%H:%M:%S")))
        parmLocDict = {
            "tmp":{"start":15,"stop":19},
            "dpt":{"start":21,"stop":25},
            "wsp":{"start":39,"stop":43},
            "q06":{"start":56,"stop":61},
            "q24":{"start":None,"stop":None},
            }
        with gzip.open(self.destFiles["primary"]) as gzf:
            gzData = gzf.readlines()
        self.obDict = {}
        for obRow in gzData:
            dateKey = datetime(int(obRow[:4]),int(obRow[5:7]),int(obRow[8:10]),int(obRow[11:13]))
            if str(dateKey) in self.dataDates:
                self.obDict[str(dateKey)] = {}
                for eachParm in list(parmLocDict.keys()):
                    if eachParm in ["tmp","dpt"]:
                        if int(obRow[parmLocDict[eachParm]["start"]:parmLocDict[eachParm]["stop"]]) == -9999:
                            convVal = "null"
                        else:
                            # C to F
                            convVal = round(((int(obRow[parmLocDict[eachParm]["start"]:parmLocDict[eachParm]["stop"]])/10) * (9/5)) + 32)
                    elif eachParm in ["wsp"]:
                        if int(obRow[parmLocDict[eachParm]["start"]:parmLocDict[eachParm]["stop"]]) == -9999:
                            convVal = "null"
                        else:
                            # m/s to kts
                            convVal = round((int(obRow[parmLocDict[eachParm]["start"]:parmLocDict[eachParm]["stop"]])/10) * 1.94384)
                    elif eachParm in ["q06"]:
                        # mm -> in
                        if int(obRow[parmLocDict[eachParm]["start"]:parmLocDict[eachParm]["stop"]]) == -9999:
                            convVal = "null"
                        else:
                            convVal = round((int(obRow[parmLocDict[eachParm]["start"]:parmLocDict[eachParm]["stop"]])/10) * 0.0393701,2)
                    elif eachParm in ["q24"]:
                        # Placeholder - to be calculated off q06 at a later time
                        convVal = "null"
                    self.obDict[str(dateKey)][eachParm] = convVal
        os.remove(self.destFiles["primary"])

    def logWindAdj(self, spd):
        if self.windAdjMethod == "ndbc":
            return spd*1.94384
        d = 0
        z1 = 10
        z2 = self.stations[self.stn]["elev"]
        z0 = 2.0e-4
        k = 0.40
        ## https://www.sjsu.edu/people/frank.freedman/courses/metr130/s1/Met130_Spring11_Lect3.pdf <-- For eqns below
        # adjWnd = (spd/k)*(math.log((z-d)/z0)) ## Eqn from Ryan's presentation.
        adjWnd = spd*((math.log((z1-d)/z0))/(math.log((z2-d)/z0))) ## Uz1/Uz2 = ln[(z1-d)/z0] / ln[(z2-d)/z0] --> Uz1 = Uz2 * ( ln[(z1-d)/z0] / ln[(z2-d)/z0] )
        adjWndKts = adjWnd * 1.93384
        return adjWndKts

    def writeData(self):
        print("( {:s} ) - Writing final obs dictionary to file: {:s}".format(datetime.utcnow().strftime("%H:%M:%S"),self.stations[self.stn]["ob"]))
        kysForSorting = [ datetime.strptime(x,"%Y-%m-%d %H:%M:%S") for x in list(self.obDict.keys()) ]
        kysForSorting.sort()
        sorted_dict = {str(i):self.obDict[str(i)] for i in kysForSorting}
        with open(self.stations[self.stn]["ob"],"w") as outfile:
            fmtJsonStr = json.dumps(sorted_dict,indent=4,sort_keys=False)
            outfile.write(fmtJsonStr)
        return

ObGetter()