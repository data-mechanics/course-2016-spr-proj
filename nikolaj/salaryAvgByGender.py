import subprocess

subprocess.call(['mongo repo -u nikolaj -p nikolaj --authenticationDatabase "repo" < sal.js'], shell=True)
print('done.')
