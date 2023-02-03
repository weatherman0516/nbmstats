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
        print("\033[1;33m( {:s} ) - Working on NBM data.\033[0;0m".format(datetime.utcnow().strftime("%H:%M:%S")))
        print("( {:s} ) - Downloading, reading, and meshing NBM text bulletins...".format(datetime.utcnow().strftime("%H:%M:%S")))
        self.dateCounter = 1
        for self.eachDate in self.dateList:
            self.hrCounter = 1
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
                self.hrCounter += 1
            self.dateCounter += 1
        sys.stdout.write("\n\033[1;32m( {:s} ) - Complete!\033[0;0m\n".format(datetime.utcnow().strftime("%H:%M:%S")))
        self.writeData()
        return

    def readBulletins(self):

        for eachFile in self.filesToRead:
            bullRegex = re.compile(r".*blend_(.*)tx\.t.*")
            self.thisBull = re.search(bullRegex,eachFile)
            with open(eachFile,'r') as bullFile:
                bull = bullFile.readlines()
                stationCounter = 1
                for self.stn in list(self.stations.keys()):
                    sys.stdout.write("\r")
                    sys.stdout.write("( %s ) - Date %s - Hour %s - Station %s (%s) - Bulletin: %s" % (datetime.utcnow().strftime("%H:%M:%S"), str(self.dateCounter)+"/"+str(len(self.dateList)),str(self.hrCounter)+"/"+str(len(self.hours)),str(stationCounter)+"/"+str(len(list(self.stations.keys()))),self.stn,self.thisBull.group(1)))
                    sys.stdout.flush()
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
                    for self.parmKey in list(self.bulletinDict["nbe"]["fieldIdx"].keys()):
                        theLine = self.handleLine()
                        self.orgData[self.stn][self.thisBull.group(1)][self.parmKey] = theLine
                    dateLine = self.calcDates()
                    self.orgData[self.stn][self.thisBull.group(1)]["dat"] = dateLine
                    stationCounter += 1
            os.remove(eachFile)
        self.meshBulletins()


    def handleLine(self):
        if self.thisBull.group(1) == "nbh" and self.parmKey == "fhr":
            # The NBH bulletin does not have a forecast hour field. But is 25 hours of data starting with start hour + 1. So we make a formatted fhr array here.
            cleanedArr = [x for x in range(1,26)]
        elif self.thisBull.group(1) == "nbh" and (self.parmKey == "txn" or self.parmKey == "q24" or self.parmKey == "q06"):
            # The NBH bulletin does not have data for 18hr mx/mn T or for 12 hr qpf. Fill an array of NA for ease of comparison later.
            cleanedArr = ["NA" for x in range(1,26)]
        elif self.thisBull.group(1) == "nbs" and (self.parmKey == "q24"):
            # The NBS bulletin does not have data for 24 hr qpf. Fill an array of NA for ease of comparison later. NBS has fewer entries than NBH
            cleanedArr = ["NA" for x in range(1,24)]
        elif self.thisBull.group(1) == "nbe" and (self.parmKey == "q06"):
            # The NBE bulletin does not have data for 06 hr qpf. Fill an array of NA for ease of comparison later. NBE has fewer entries than NBH/NBS
            cleanedArr = ["NA" for x in range(1,16)]
        else:
            delimNum = self.bulletinDict[self.thisBull.group(1)]["charDelimit"]
            # Grab each line (AFTER THE PARM TITLE COLUMN)
            thisLine = self.subArr[self.bulletinDict[self.thisBull.group(1)]["fieldIdx"][self.parmKey]][self.bulletinDict[self.thisBull.group(1)]["lpad"]:]
            # Break the line into an array, split by pre defined character limit
            thisArr = [thisLine[i:i+delimNum] for i in range(0,len(thisLine),delimNum)]
            if self.parmKey == "txn" or self.parmKey == "q06" or self.parmKey == "q24":
                # wipe out all spaces and get rid of the trailing line break
                almostCleanedArr = [ re.sub('[^0-9]','', x) for x in thisArr if not x == "\n"]
                # replace empty array elements (those that were just spaces) with NA for consistency with nbh
                if self.parmKey == "q06" or self.parmKey == "q24":
                    cleanedArr = [ str(round(int(x)/100,2)) if x != '' else 'NA' for x in almostCleanedArr ]
                else:
                    cleanedArr = [ x if x != '' else 'NA' for x in almostCleanedArr ]
            else:
                # Clear all non numeric characters and remove empty strings (that came at the end <- though for now being at the end isnt explicitly defined)
                cleanedArr = [ re.sub('[^0-9]','', x) for x in thisArr if not x.isspace() ]
        return cleanedArr

    def calcDates(self):
        # Create a date array that corresponds to the values that is the actual date based on the start hour and forecast hour
        dateArr = [ self.startDate+timedelta(hours=int(x)) for x in self.orgData[self.stn][self.thisBull.group(1)]["fhr"] ]
        return dateArr

    def meshBulletins(self):
        orderedBulletins = ["nbh","nbs","nbe"]
        # Make a few emtpy arrays
        tempDict = {}
        for eachParm in list(self.bulletinDict["nbe"]["fieldIdx"].keys()):
            tempDict[eachParm] = []
        tempDat = []
        for self.site in self.orgData:
            for self.bullToMesh in orderedBulletins:
                for i, val in enumerate(self.orgData[self.site][self.bullToMesh]["dat"]):
                    if str(val) not in tempDat:
                        tempDat.append(str(val))
                        for eachParm in list(self.bulletinDict["nbe"]["fieldIdx"].keys()):
                            tempDict[eachParm].append(self.orgData[self.site][self.bullToMesh][eachParm][i])
                    # The hope here is that if the date is already in the meshed list, but we're looking at the nbs,
                    # then we want to put nbs values in the appropriate location for the fields that nbh doesn't produce
                    # for the period where there's time overlap with the nbs
                    elif (str(val) in tempDat) and (self.bullToMesh == "nbs"):
                        # Get the index of the date that already exists in the array so we can change the parm value from NA to the NBS value.
                        subIdx = tempDat.index(str(val))
                        # Do this for each of the parms that NBH does not have
                        for eachParm in ["txn","q06"]:
                            # Change the value
                            tempDict[eachParm][subIdx] = self.orgData[self.site][self.bullToMesh][eachParm][i]
                    elif (str(val) in tempDat) and (self.bullToMesh == "nbe"):
                        # Get the index of the date that already exists in the array so we can change the parm value from NA to the NBE value.
                        subIdx = tempDat.index(str(val))
                        # Do this for each of the parms that NBH/NBS does not have
                        for eachParm in ["q24"]:
                            # Change the value
                            tempDict[eachParm][subIdx] = self.orgData[self.site][self.bullToMesh][eachParm][i]
            if self.site not in self.meshedDict:
                self.meshedDict[self.site] = {}
            self.meshedDict[self.site][str(self.startDate)] = {}
            for loc,eachFhr in enumerate(tempDict["fhr"]):
                self.meshedDict[self.site][str(self.startDate)][str(eachFhr)] = {}
                updatedList = [ ky for ky in list(tempDict.keys()) if ky != "fhr" ]
                for eachParm in updatedList:
                    if eachParm not in self.meshedDict[self.site][str(self.startDate)][str(eachFhr)]:
                        self.meshedDict[self.site][str(self.startDate)][str(eachFhr)][eachParm] = {}
                    self.meshedDict[self.site][str(self.startDate)][str(eachFhr)][eachParm]["date"] = tempDat[loc]
                    if tempDict[eachParm][loc] != "NA":
                        self.meshedDict[self.site][str(self.startDate)][str(eachFhr)][eachParm]["model"] = float(tempDict[eachParm][loc])
                    else:
                        self.meshedDict[self.site][str(self.startDate)][str(eachFhr)][eachParm]["model"] = "null"
        return

    def writeData(self):
        for eachStation in self.stations:
            kysForSorting = [ datetime.strptime(x,"%Y-%m-%d %H:%M:%S") for x in list(self.meshedDict[self.site].keys()) ]
            kysForSorting.sort()
            sorted_dict = {str(i):self.meshedDict[self.site][str(i)] for i in kysForSorting}
            with open(self.stations[eachStation]["model"],"w") as outfile:
                fmtJsonStr = json.dumps(sorted_dict,indent=4,sort_keys=False)
                outfile.write(fmtJsonStr)
        return

BulletinDecoder()