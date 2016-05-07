import urllib.request
import ast
import statistics
# import matplotlib.pyplot as plt
import numpy as np
# import 
import plotly.plotly as py
import plotly.graph_objs as go

lu = {'green' : 'gr', 'green-b' : 'gr-b', 'green-c' : 'gr-c', 'green-d' : 'gr-d', 'green-e' : 'gr-e', 'orange' : 'org', 'blue': 'blue'}
def abrv(rt):
	if rt in lu:
		return lu[rt]
	else:
		return rt

def one_to_zero(sr):
	if sr == 1.0:
		return 0.0
	else:
		return sr

url = "http://datamechanics.io/data/nikolaj/route-simrank.json"
with urllib.request.urlopen(url) as loaded:
    response = loaded.read().decode("utf-8")
    simranks = ast.literal_eval(response)

ranks_2d = []
routes = []
for route, simranks in sorted([list(entry.items())[0] for entry in simranks]):
    total_simrank = sum([sr for rt, sr in list(simranks.items())])
    sorted_by_rank = sorted(list(simranks.items()), key=lambda x: -x[1])
    sorted_by_route = sorted(list(simranks.items()), key=lambda x: x[0])
    # sorted_by_rank = sorted_by_rank[1:]
    rank_only = [one_to_zero(sr) for rt, sr in sorted_by_route]
    ranks_2d.append(rank_only)
    routes.append(route)
    # sr_mean = statistics.mean(rank_only)
    # sr_std = statistics.stdev(rank_only)
    # out = str([abrv(rt) for rt, sr in sorted_by_rank]).replace("'", "")
    # print(abrv(route), min(rank_only), max(rank_only), sr_mean, sr_std)
print(ranks_2d)
print(routes)

layout = go.Layout(
    title="MBTA Route SimRank",  # set plot's title
    font=go.Font(
        family="Droid Sans, sans-serif",
    ),
    xaxis=go.XAxis(
        title='Routes',  # x-axis title
        showgrid=False             # remove grid
    ),
    yaxis=go.YAxis(
        title='Routes', # y-axis title
        # autorange='reversed',  # (!) reverse tick ordering
        showgrid=False,   # remove grid
        autotick=False,   # custom ticks
        dtick=1           # show 1 tick per day
    )
    # autosize=False,  # custom size
    # height=height,   # plot's height in pixels 
    # width=width      # plot's width in pixels
)
py.sign_in('PythonAPI', 'ubpiol2cve')
data = [
    go.Heatmap(
        z=ranks_2d,
        x=routes,
        y=routes,
        colorscale='Hot',
        reversescale=True
    )
]

plot_url = py.plot(data, filename='basic-heatmap')
print(plot_url)
