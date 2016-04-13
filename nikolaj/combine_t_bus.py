import subprocess

def run():
    returncode = subprocess.call(["mongo repo -u nikolaj -p nikolaj < combine_t_bus.js"], shell=True)
    return returncode
