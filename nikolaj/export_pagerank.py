import subprocess

def run():
    export_cmd = "mongoexport --db repo --collection nikolaj.pagerank_result --out ranks.json"
    returncode = subprocess.call([export_cmd], shell=True)
    with open('ranks.json') as f:
        raw = f.read()
        raw = '[' + raw.replace('\n', ',\n')[:-2] + ']'
    with open('ranks.json', 'w') as f:
        f.write(raw)

if __name__ == '__main__':
	run()
