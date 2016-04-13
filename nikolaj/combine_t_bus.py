import subprocess
import pymongo
exec(open('../pymongo_dm.py').read())

def get_auth_repo(uname, pwd):
    client = pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(uname, pwd)
    return repo

def run():
    returncode = subprocess.call(["mongo repo -u nikolaj -p nikolaj < combine_t_bus.js"], shell=True)
    return returncode
