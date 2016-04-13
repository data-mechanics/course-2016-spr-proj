import subprocess

def run():
    returncode = subprocess.call(["mongo repo -u nikolaj -p nikolaj < combineforviz.js"], shell=True)
    return returncode

if __name__ == "__main__":
	run()
