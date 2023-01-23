#!/usr/bin/env python3

dateRange = ("20230117", "20230122")

dataDir = "/home/weatherman0516/nbmstats/data/"

stations = {"LKWF1":{"model":dataDir+"LKWF1.json",
                    "ob":dataDir+"LKWF1_obs.json",
                    "bias":dataDir+"LKWF1_bias.json",
                    "elev":6.0 #in m
                    },
            "SPGF1":{"model":dataDir+"SPGF1.json",
                    "ob":dataDir+"SPGF1_obs.json",
                    "bias":dataDir+"SPGF1_bias.json",
                    "elev":6.6 #in m
                    },
            "VAKF1":{"model":dataDir+"VAKF1.json",
                    "ob":dataDir+"VAKF1_obs.json",
                    "bias":dataDir+"VAKF1_bias.json",
                    "elev":10.2 #in m
                    },
            "41009":{"model":dataDir+"41009.json",
                    "ob":dataDir+"41009_obs.json",
                    "bias":dataDir+"41009_bias.json",
                    "elev":4.1 #in m
                    },
            "41010":{"model":dataDir+"41010.json",
                    "ob":dataDir+"41010_obs.json",
                    "bias":dataDir+"41010_bias.json",
                    "elev":4.1 #in m
                    },
            "KYWF1":{"model":dataDir+"KWYF1.json",
                    "ob":dataDir+"KYWF1_obs.json",
                    "bias":dataDir+"KYWF1_bias.json",
                    "elev":15.0 #in m
                    },
            }

hours = ["01","11"]

bulletinDict = {"nbh":{"numRows":38,"lpad":5,"charDelimit":3,"fieldIdx":{"fhr":None,"wsp":9,"wgs":11}},
                "nbs":{"numRows":42,"lpad":8,"charDelimit":3,"fieldIdx":{"fhr":3,"wsp":13,"wgs":15}},
                "nbe":{"numRows":31,"lpad":7,"charDelimit":4,"fieldIdx":{"fhr":3,"wsp":13,"wgs":15}}
                }
