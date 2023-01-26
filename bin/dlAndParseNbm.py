#!/usr/bin/env python3

import stationConfig
import re, json, urllib.request, os, sys
from datetime import datetime, timedelta

class BulletinDecoder:
    def __init__(self):
        self.bulletinDict = stationConfig.bulletinDict
        # self.dateRange = stationConfig.dateRange
        self.dateRange = (str(sys.argv[1]),str(sys.argv[2]))
        self.hours = stationConfig.hours
        self.stations = stationConfig.stations
        self.orgData = {}
        self.meshedDict = {}
        self.dateList = self.makeDateList()
        self.dlBulletin()

    def makeDateList(self):
        dateList = []
        startDate = datetime.strptime(self.dateRange[0],"%Y%m%d")
        endDate = datetime.strptime(self.dateRange[1],"%Y%m%d")
        while startDate <= endDate:
            dateList.append(startDate.strftime("%Y%m%d"))
            startDate = startDate + timedelta(days=1)
        return dateList

    def dlBulletin(self):
        urlPre = "https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend."
        # blend_<bull>tx.t<hh>z
        filePre = "../data/blend_"
        fileMid = "tx.t"
        fileSuff = "z"
        for self.eachDate in self.dateList:
            for self.eachHr in self.hours:
                urlWithDate = urlPre + self.eachDate + "/" + self.eachHr + "/text/"
                self.filesToRead = []
                for self.bullTyp in self.bulletinDict:
                    fileName = "blend_" + self.bullTyp + fileMid + self.eachHr + fileSuff
                    fullUrl = urlWithDate + fileName
                    try:
                        opener = urllib.request.build_opener()
                        urllib.request.install_opener(opener)
                        self.destFile = os.path.join("/home/weatherman0516/nbmstats/data",fileName)
                        urllib.request.urlretrieve(fullUrl,self.destFile)
                        self.filesToRead.append(self.destFile)
                    except Exception as e:
                        print(fileName+" for "+self.eachDate+" is not available.")
                self.readBulletins()
        self.writeData()
        return

    def readBulletins(self):

        for eachFile in self.filesToRead:
            bullRegex = re.compile(r".*blend_(.*)tx\.t.*")
            self.thisBull = re.search(bullRegex,eachFile)
            with open(eachFile,'r') as bullFile:
                bull = bullFile.readlines()
                for self.stn in list(self.stations.keys()):
                    # Looking for something like " LKWF1    NBM"
                    pat = re.compile(r"("+re.escape(self.stn)+r"\s*NBM.*$)")
                    # Row number of the start of the station bulletin
                    seeker = [ i for i,val in enumerate(bull) if re.search(pat,val) ]
                    # All rows associated with the station
                    self.subArr = bull[seeker[0]:seeker[0]+self.bulletinDict[self.thisBull.group(1)]["numRows"]]
                    datePat = re.compile(r"([0-9]+\/[0-9]+\/[0-9]{4}  [0-9]{2})")
                    strippedDate = re.search(datePat,self.subArr[0])
                    self.startDate = datetime.strptime(strippedDate.group(1),"%m/%d/%Y  %H")
                    if self.stn not in self.orgData:
                        self.orgData[self.stn] = {}
                    if self.bullTyp not in self.orgData[self.stn]:
                        self.orgData[self.stn][self.thisBull.group(1)] = {}
                    # for self.parmKey in self.orgData[self.stn][self.bullTyp]:
                    for self.parmKey in list(self.bulletinDict["nbh"]["fieldIdx"].keys()):
                        theLine = self.handleLine()
                        self.orgData[self.stn][self.thisBull.group(1)][self.parmKey] = theLine
                    dateLine = self.calcDates()
                    self.orgData[self.stn][self.thisBull.group(1)]["dat"] = dateLine
            os.remove(eachFile)
        self.meshBulletins()


    def handleLine(self):
        if self.thisBull.group(1) == "nbh" and self.parmKey == "fhr":
            # The NBH bulletin does not have a forecast hour field. But is 25 hours of data starting with start hour + 1. So we make a formatted fhr array here.
            cleanedArr = [x for x in range(1,26)]
        else:
            delimNum = self.bulletinDict[self.thisBull.group(1)]["charDelimit"]
            # Grab each line (AFTER THE PARM TITLE COLUMN)
            thisLine = self.subArr[self.bulletinDict[self.thisBull.group(1)]["fieldIdx"][self.parmKey]][self.bulletinDict[self.thisBull.group(1)]["lpad"]:]
            # Break the line into an array, split by pre defined character limit
            thisArr = [thisLine[i:i+delimNum] for i in range(0,len(thisLine),delimNum)]
            # Clear all non numeric characters and remove empty strings (that came at the end <- though for now being at the end isnt explicitly defined)
            cleanedArr = [ re.sub('[^0-9]','', x) for x in thisArr if not x.isspace() ]
        return cleanedArr

    def calcDates(self):
        # Create a date array that corresponds to the values that is the actual date based on the start hour and forecast hour
        dateArr = [ self.startDate+timedelta(hours=int(x)) for x in self.orgData[self.stn][self.thisBull.group(1)]["fhr"] ]
        return dateArr

    def meshBulletins(self):
        # Make a few emtpy arrays
        tempDict = {}
        for eachParm in list(self.bulletinDict["nbh"]["fieldIdx"].keys()):
            tempDict[eachParm] = []
        tempDat = []
        for self.site in self.orgData:
            for self.bullToMesh in self.orgData[self.site]:
                for i, val in enumerate(self.orgData[self.site][self.bullToMesh]["dat"]):
                    if str(val) not in tempDat:
                        tempDat.append(str(val))
                        tempDict["fhr"].append(self.orgData[self.site][self.bullToMesh]["fhr"][i])
                        tempDict["wsp"].append(self.orgData[self.site][self.bullToMesh]["wsp"][i])
                        tempDict["wgs"].append(self.orgData[self.site][self.bullToMesh]["wgs"][i])
            if self.site not in self.meshedDict:
                self.meshedDict[self.site] = {}
            self.meshedDict[self.site][str(self.startDate)] = {}
            for loc,eachFhr in enumerate(tempDict["fhr"]):
                self.meshedDict[self.site][str(self.startDate)][eachFhr] = {}
                updatedList = [ ky for ky in list(tempDict.keys()) if ky != "fhr" ]
                for eachParm in updatedList:
                    if eachParm not in self.meshedDict[self.site][str(self.startDate)][eachFhr]:
                        self.meshedDict[self.site][str(self.startDate)][eachFhr][eachParm] = {}
                    self.meshedDict[self.site][str(self.startDate)][eachFhr][eachParm]["date"] = tempDat[loc]
                    self.meshedDict[self.site][str(self.startDate)][eachFhr][eachParm]["model"] = float(tempDict[eachParm][loc])
        return

    def writeData(self):
        for eachStation in self.stations:
            with open(self.stations[eachStation]["model"],"w") as outfile:
                json.dump(self.meshedDict[self.site],outfile)
        return

BulletinDecoder()