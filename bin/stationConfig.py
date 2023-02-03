#!/usr/bin/env python3

# Station id lookup file
# https://www.ncei.noaa.gov/pub/data/noaa/isd-history.txt
# Example of data folder to grab an annual file by station id
# https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/2022/
# File discussing format of above
# https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/isd-lite-format.txt

dataDir = "/home/weatherman0516/nbmstats/data/"

## Set to True if you want to create a .json file with bias information
## without any conditional filtering
biasData = True
## Set to True if you want to filter some of the bias data based on a
## threshold of your choosing. Then choose the value and operator you'd
## like for the filtering.
conditionalData = True
compOperator = "ge" # eq ne ge gt le lt
compVal = 15.0

stations = {
            # "LKWF1":{"model":dataDir+"LKWF1.json",
            #         "ob":dataDir+"LKWF1_obs.json",
            #         "paired":dataDir+"LKWF1_paired.json",
            #         "bias":dataDir+"LKWF1_bias.json",
            #         "conditional":dataDir+"LKWF1_condl.json",
            #         "conditionalFalse":dataDir+"LKWF1_condlFalse.json",
            #         "elev":6.0, #in m
            #         "type":"marine" #or land
            #         },
            "KMIA":{"model":dataDir+"KMIA.json",
                    "ob":dataDir+"KMIA_obs.json",
                    "paired":dataDir+"KMIA_paired.json",
                    "bias":dataDir+"KMIA_bias.json",
                    "conditional":dataDir+"KMIA_condl.json",
                    "conditionalFalse":dataDir+"KMIA_condlFalse.json",
                    "elev":10.0, #in m
                    "type":"land" #or marine
                    },
            "KFLL":{"model":dataDir+"KFLL.json",
                    "ob":dataDir+"KFLL_obs.json",
                    "paired":dataDir+"KFLL_paired.json",
                    "bias":dataDir+"KFLL_bias.json",
                    "conditional":dataDir+"KFLL_condl.json",
                    "conditionalFalse":dataDir+"KFLL_condlFalse.json",
                    "elev":10.0, #in m
                    "type":"land" #or marine
                    },
            "KPBI":{"model":dataDir+"KPBI.json",
                    "ob":dataDir+"KPBI_obs.json",
                    "paired":dataDir+"KPBI_paired.json",
                    "bias":dataDir+"KPBI_bias.json",
                    "conditional":dataDir+"KPBI_condl.json",
                    "conditionalFalse":dataDir+"KPBI_condlFalse.json",
                    "elev":10.0, #in m
                    "type":"land" #or marine
                    },
            # "SPGF1":{"model":dataDir+"SPGF1.json",
            #         "ob":dataDir+"SPGF1_obs.json",
            #         "paired":dataDir+"SPGF1_paired.json",
            #         "bias":dataDir+"SPGF1_bias.json",
            #         "conditional":dataDir+"SPGF1_condl.json",
            #         "conditionalFalse":dataDir+"SPGF1_condlFalse.json",
            #         "elev":6.6 #in m
            #         },
            # "VAKF1":{"model":dataDir+"VAKF1.json",
            #         "ob":dataDir+"VAKF1_obs.json",
            #         "paired":dataDir+"VAKF1_paired.json",
            #         "bias":dataDir+"VAKF1_bias.json",
            #         "conditional":dataDir+"VAKF1_condl.json",
            #         "conditionalFalse":dataDir+"VAKF1_condlFalse.json",
            #         "elev":10.2 #in m
            #         },
            # "41009":{"model":dataDir+"41009.json",
            #         "ob":dataDir+"41009_obs.json",
            #         "paired":dataDir+"41010_paired.json",
            #         "bias":dataDir+"41009_bias.json",
            #         "conditional":dataDir+"41009_condl.json",
            #         "conditionalFalse":dataDir+"41009_condlFalse.json",
            #         "elev":4.1 #in m
            #         },
            # "41010":{"model":dataDir+"41010.json",
            #         "ob":dataDir+"41010_obs.json",
            #         "paired":dataDir+"41010_paired.json",
            #         "bias":dataDir+"41010_bias.json",
            #         "conditional":dataDir+"41010_condl.json",
            #         "conditionalFalse":dataDir+"41010_condlFalse.json",
            #         "elev":4.1 #in m
            #         },
            # "KYWF1":{"model":dataDir+"KWYF1.json",
            #         "ob":dataDir+"KYWF1_obs.json",
            #         "paired":dataDir+"KYWF1_paired.json",
            #         "bias":dataDir+"KYWF1_bias.json",
            #         "conditional":dataDir+"KYWF1_condl.json",
            #         "conditionalFalse":dataDir+"KYWF1_condlFalse.json",
            #         "elev":15.0 #in m
            #         },
            # "SMKF1":{"model":dataDir+"SMKF1.json",
            #         "ob":dataDir+"SMKF1_obs.json",
            #         "paired":dataDir+"SMKF1_paired.json",
            #         "bias":dataDir+"SMKF1_bias.json",
            #         "conditional":dataDir+"SMKF1_condl.json",
            #         "conditionalFalse":dataDir+"SMKF1_condlFalse.json",
            #         "elev":7.2 #in m
            #         },
            # "VCAF1":{"model":dataDir+"VCAF1.json",
            #         "ob":dataDir+"VCAF1_obs.json",
            #         "paired":dataDir+"VCAF1_paired.json",
            #         "bias":dataDir+"VCAF1_bias.json",
            #         "conditional":dataDir+"VCAF1_condl.json",
            #         "conditionalFalse":dataDir+"VCAF1_condlFalse.json",
            #         "elev":6.5 #in m
            #         },
            # "LONF1":{"model":dataDir+"LONF1.json",
            #         "ob":dataDir+"LONF1_obs.json",
            #         "paired":dataDir+"LONF1_paired.json",
            #         "bias":dataDir+"LONF1_bias.json",
            #         "conditional":dataDir+"LONF1_condl.json",
            #         "conditionalFalse":dataDir+"LONF1_condlFalse.json",
            #         "elev":6.34 #in m
            #         },
            }

windAdjMethod = "ndbc" ## ndbc or equation to use ndbc's adjusted value or the log wind profile respectively

# Need to look at FHR logic since the date holds steady in the NBE bulletin but the fhr changes every hour
# hours = ["01","11"]
hours = ["11"]

bulletinDict = {"nbh":{"numRows":38,"lpad":5,"charDelimit":3,"fieldIdx":{"fhr":None,"tmp":2,"dpt":4,"wsp":9,"wgs":11,"q06":None}},
                "nbs":{"numRows":42,"lpad":8,"charDelimit":3,"fieldIdx":{"fhr":3,"txn":4,"tmp":6,"dpt":8,"wsp":13,"wgs":15,"q06":19}},
                "nbe":{"numRows":31,"lpad":7,"charDelimit":4,"fieldIdx":{"fhr":3,"txn":4,"tmp":6,"dpt":8,"wsp":13,"wgs":15,"q24":19,"q06":None}}
                }
