import urllib.request
import ast

def O_from_I(I):
    O = {}
    for node, neighs in I.items():
        for neigh in neighs:
            if neigh not in O:
                O[neigh] = set()
            O[neigh].add(node)
    return O

def init_sim_table(adj):
    S = {}
    for node in adj.keys():
        S[node] = dict([(other, 0.0) for other in adj.keys()])
        S[node][node] = 1.0
    return S

# NOTE: S gets modified
def _bipartite_simrank(S, other_S, neighs, C):
    for a in S.keys():
        for b in S.keys():
            if a == b:
                continue
            new_sim_rank = 0.0
            for a_n in neighs[a]:
                for b_n in neighs[b]:
                    new_sim_rank += other_S[a_n][b_n]
            S[a][b] = (C / (len(neighs[a]) * len(neighs[b]))) * new_sim_rank

def get_stations(url):
    with urllib.request.urlopen(url) as loaded:
        response = loaded.read().decode("utf-8")
        stations = ast.literal_eval(response) 
        return dict([(st["id"], set(st["routes"])) for st in stations])

def bipartite_simrank(graph):
    I = graph
    O = O_from_I(I)
    S1_points_to = init_sim_table(O)
    S2_pointed_to = init_sim_table(I)
    S1_updated = init_sim_table(O)
    S2_updated = init_sim_table(I)

    for i in range(5):
        _bipartite_simrank(S1_updated, S2_pointed_to, O, 0.8)
        _bipartite_simrank(S2_updated, S1_points_to, I, 0.8)
        S1_points_to = S1_updated
        S2_pointed_to = S2_updated
        # TODO: add convergence check 

    return S1_points_to, S2_pointed_to

graph = get_stations('http://datamechanics.io/data/nikolaj/ranks-with-bus.json')
# graph = {'sugar': set(['A']), 'frosting': set(['A', 'B']), 'eggs': set(['A', 'B']), 'flour': set(['B'])}
S1_points_to, S2_pointed_to = bipartite_simrank(graph)
json_simrank = [dict([s]) for s in S1_points_to.items()]

with open('route-simrank.json', 'w') as f:
    f.write(str(json_simrank).replace("'", '"'))
# print(S1_points_to)
# print(S2_pointed_to)
