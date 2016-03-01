def run(repo):

	repo.dropPermanent("assessments_2014")
	repo.createPermanent("assessments_2014")
	repo.dropPermanent("assessments_2015")
	repo.createPermanent("assessments_2015")
	repo.dropPermanent("approved_permits")
	repo.createPermanent("approved_permits")
	repo.dropPermanent("neighborhood_zoning")
	repo.createPermanent("neighborhood_zoning")
	repo.dropPermanent("residential_centers")
	repo.createPermanent("residential_centers")
	repo.dropPermanent("commercial_centers")
	repo.createPermanent("commercial_centers")
	repo.dropPermanent("ptypes")
	repo.createPermanent("ptypes")

#EOF