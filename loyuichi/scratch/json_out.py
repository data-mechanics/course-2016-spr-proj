import json

data = { "02201": {"count" : 1},
	  "02121": {"count" : 9},
	  "02124": {"count" : 28},
	  "02128": {"count" : 58},
	  "02122": {"count" : 20},
	  "02135": {"count" : 40},
	  "02110": {"count" : 52},
	  "02117": {"count" : 1},
	  "02116": {"count" : 100},
	  "02163": {"count" : 2},
	  "02130": {"count" : 41},
	  "02119": {"count" : 26},
	  "02210": {"count" : 38},
	  "02114": {"count" : 48},
	  "02120": {"count" : 10},
	  "02115": {"count" : 54},
	  "02199": {"count" : 5},
	  "02127": {"count" : 33},
	  "02467": {"count" : 1},
	  "02109": {"count" : 47},
	  "02108": {"count" : 41},
	  "02129": {"count" : 16},
	  "02134": {"count" : 47},
	  "02126": {"count" : 10},
	  "02111": {"count" : 65},
	  "02215": {"count" : 43},
	  "02136": {"count" : 14},
	  "02118": {"count" : 28},
	  "02125": {"count" : 27},
	  "02113": {"count" : 49},
	  "02131": {"count" : 29},
	  "02132": {"count" : 17}}  
	  
with open('test.json', 'w') as outfile:
    out = "var food_est_counts = "
    out += json.dumps(data)
    outfile.write(out)
