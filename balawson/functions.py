def c_rp(first_visit, second_visit):
    N = 0 #number of animals in the population
    n = len(first_visit)  #number of animals marked on first visit
    K = len(second_visit) #number of animals captured on the second visit
    k = sum([1 if x in first_visit else 0 for x in second_visit])
    try:
        N = (K*n) / float(k)
    except ZeroDivisionError:
        #N = (K*n) / (float(k) + 1)
         N = 0
    return N
