import json

class Convert:
	def __init__(self, tar):
		file = open(tar)
		self.json = json.loads(file.read())
		file.close()
		self.data = [self.makeDict(row) for row in self.json['data'] ]

	def makeDict(self, row):
		def aux (columnData, row):
			dict = {}
			for i, v in enumerate(row):
				if 'subColumnTypes' in columnData[i]:
					iv = {}
					for j, sv in enumerate(v):
						iv[columnData[i]['subColumnTypes'][j]] = sv
					v = iv
				dict[columnData[i]['name']] = v
			return dict
		return aux(self.json['meta']['view']['columns'], row)

	def data(self):
		return self.data

	def write(self, fileName, start=0, stop=-1):
		if stop == -1:
			stop = len(self.data)
		f = open(fileName, 'w')
		f.write(json.dumps(self.data[start:stop], indent=4, separators=(',', ': ')))
		f.close()