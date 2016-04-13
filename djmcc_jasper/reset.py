###############################################
# Title:   reset.py
# Authors: Daren McCulley and Jasper Burns
# Purpose: Database wipe and re-initialization 
###############################################

def run(repo):

	print('Dropping collections...')
	repo.dropPermanent('apartments')
	repo.dropPermanent('collapsed_apartments')
	repo.dropPermanent('filtered_apartments')
	repo.dropPermanent('assessments')
	repo.dropPermanent('filtered_assessments')
	repo.dropPermanent('merged_assessments')
	repo.dropPermanent('joined')
	repo.dropPermanent('web_data')

	print('Creating collections...')
	repo.createPermanent('apartments')
	repo.createPermanent('collapsed_apartments')
	repo.createPermanent('filtered_apartments')
	repo.createPermanent('assessments')
	repo.createPermanent('filtered_assessments')
	repo.createPermanent('merged_assessments')
	repo.createPermanent('joined')
	repo.createPermanent('web_data')

#EOF
