import json
import elementaryOperations as eo
import stats as stats


# uncomment code below in order to create json files


# k = eo.intersectHJ[0]
#
# keyList = list(k.keys())
# #
# combined = {}
# for key in keyList:
#     if key == "_id":
#         continue
#     combined.update({key: k[key]})
#
# with open("./visualizations/physicalData/jamHospiCount.json", "w") as f:
#     json.dump(combined, f)


# countValue = stats.countValue
# countValueKeys = list(countValue.keys())
#
# combinedTwo = {}
#
# for key in countValueKeys:
#     combinedTwo.update({key: [countValue[key][0], countValue[key][1]]})
#
# with open("./visualizations/physicalData/countValue.json", "w") as f:
#     json.dump(combinedTwo, f)


# jamsValues = stats.jamsValues
# jamsValueKeys = list(jamsValues.keys())
#
# combinedThree = {}
#
# for key in jamsValueKeys:
#     combinedThree.update({key: [jamsValues[key][0], jamsValues[key][1]]})
#
# with open("./visualizations/physicalData/jamsValue.json", "w") as f:
#     json.dump(combinedThree, f)
