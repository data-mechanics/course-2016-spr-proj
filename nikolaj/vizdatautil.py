import subprocess

def export(collection, file_name):
    export_cmd = "mongoexport -u nikolaj -p nikolaj --db repo --collection nikolaj.{0} --out {1}".format(collection, file_name)
    returncode = subprocess.call([export_cmd], shell=True)
    with open(file_name) as f:
        raw = f.read()
        raw = '[' + raw.replace('\n', ',\n')[:-2] + ']'
    with open(file_name, 'w') as f:
        f.write(raw)
    
def run():
    subprocess.call(["mongo repo -u nikolaj -p nikolaj < vizdatautil.js"], shell=True)
    export('pagerank_result', 'ranks.json')
    export('pagerank_result_t_500walk_bus', 'ranks-with-bus.json')
    export('dist_routes', 'routes.json')
    
if __name__ == "__main__":
    run()
