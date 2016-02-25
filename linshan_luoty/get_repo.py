import pymongo

def get_mongodb_repo(name, pwd):
	client = pymongo.MongoClient()
	repo = client.repo
	repo.authenticate(name, pwd)

	return repo
