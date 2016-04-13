import json
import elementaryOperations as eo

k = eo.intersectHJ[0]

keyList = list(k.keys())
#
combined = {}
for key in keyList:
    if key == "_id":
        continue
    combined.update({key: k[key]})

with open("./visualizations/physicalData/jamHospiCount.json", "w") as f:
    json.dump(combined, f)
