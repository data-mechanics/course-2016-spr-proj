# # Statistical Description of Data
#####################################



#author: A. Hernadez
#http://cs.marlboro.edu/courses/spring2014/jims_tutorials/ahernandez/Apr_25.attachments/scic_stat_tests.py


####################################
#import the python file of Distribution Functions
def gammln( xx ):
    """ returns the natuaral logarithm of the gamma function at xx
        Uses: the factorial of xx-1 can be computed by doing
        math.exp( gammln( xx ) ) about eqaul to math.factorial(xx-1)
        ( This is a more efficeint computation. )
    """
    cof = [ 57.1562356658629235, -59.5979603554754912,
            14.1360979747417471, -0.491913816097620199, .339946499848118887e-4,
            .465236289270485756e-4, -.983744753048795646e-4, .158088703224912494e-3,
            -.210264441724104883e-3, .217439618115212643e-3, -.164318106536763890e-3,
            .844182239838527433e-4, -.261908384015814087e-4, .368991826595316234e-5]
    if( xx <= 0 ):
        raise Exception("bad arg in gammln")
    y=x=xx
    tmp = x+5.24218750000000000
    tmp = (x+0.5)*math.log(tmp)-tmp
    ser = 0.999999999999997092
    for j in cof:
        y+=1
        ser += j/y
    return tmp+math.log(2.5066282746310005*ser/x)

# ## Kolmogorov-Smirnov Test

def ksone( data, func ):
    """Given an array data[0..n-1], and given a user-supplied function of a single variable func 
    that is a cumulative distribution function ranging from 0 (for smallest values of its argument) 
    to 1(for largest values of its argument), this routine returns the K-S statistic d and the p-value
    prob, all as an array. Small values of prob show that the cumulative distribution function of data
    is significantly different from func. The array data is modified by being sort into ascedning order.
    """
    n = len(data)
    fo = 0.0
    ks = KSdist()
    data.sort()
    en=n
    d=0.0
    for j in range(0,n):
        fn=(j+1)/en
        ff=func(data[j])
        dt=max(abs(fo-ff),abs(fn-ff))
        if(dt>d): d=dt
        fo=fn
    en=math.sqrt(en)
    prob=ks.qks((en+0.12+0.11/en)*d)
    
    return [d,prob]

def kstwo(data1, data2):
    """Given an array data1[0..n1-1], and an array data2[0..n2-1], this routine returns the K-S
    statistic d and the p-vale prob for the null hypothesis that the data sets are drawn from the
    same distribution. Small values of prob show that the cumulative distribution function of data1
    is significantly different from the that of data2. The arrays data1 and data2 are modified by being
    sorted into ascending order.
    """
    j1=j2=0
    n1=len(data1)
    n2=len(data2)
    fn1=fn2=0.0
    ks = KSdist()
    data1.sort()
    data2.sort()
    en1=n1
    en2=n2
    d=0.0
    while(j1<n1 and j2<n2):
        d1=data1[j1]
        d2=data2[j2]
        if(d1<=d2):
            j1 += 1
            fn1 = j1/en1
            while(j1<n1 and d1==data1[j1]):
                j1 += 1
                fn1 = j1/en1
        if(d2<=d1):
            j2 += 1
            fn2 = j2/en2
            while(j2<n2 and d2==data2[j2]):
                j2 += 1
                fn2 = j2/en2
        dt=abs(fn2-fn1)
        if(dt>d): d = dt
    en=math.sqrt(en1*en2/(en1+en2))
    prob=ks.qks((en+0.12+0.11/en)*d)
    
    return [d,prob]


# ## Linear Correlation

def pearsn(x,y):
    """Given two arrays x[0..n-1] and y[0..n-1], this routine computes their correclation coefficient r 
    (returned as r), the p-value at which the null hypothesis of zero correlation is disproved (prob 
    whose small value indicates a significant correlation), and Fisher's z (return as z). whose value
    van be used in further statistical tests. r, prob and z are returned as an array.
    """
    TINY = 1.0e-20
    beta = Beta()
    n=len(x)
    yt=xt=t=df=syy=sxy=sxx=ay=ax=0.0
    for j in range(0,n):
        ax += x[j]
        ay += y[j]
    ax /= n
    ay /= n
    for j in range(0,n):
        xt=x[j]-ax
        yt=y[j]-ay
        sxx += xt*xt
        syy += yt*yt
        sxy += xt*yt
    r = sxy/(math.sqrt(sxx*syy)+TINY)
    z = 0.5*math.log((1.0+r+TINY)/(1.0-r+TINY))
    df = n-2
    t = r*math.sqrt(df/((1.0-r+TINY)*(1.0+r+TINY)))
    prob = beta.betai(0.5*df, 0.5, df/(df+t*t))
    
    return [r,prob,z]



# ## Nonparametric Correlation

# ### Spearman Rank-Order Correlation Coefficient


# ## Two-Dimensional K-S

# In[77]:

def ks2d1s(x1,y1,quadvl):
    """Two-dimensional Kolmogorov-Smirnov test of one sample against a model. Given the x and
    y coordinates of n1 data points in arrays x1[0..n1-1] and y1[0..n1-1], and given a user-
    supplied function quadvl that exemplifies the model, this routine returns the two-dimensional
    K-S statistic as d1, and its p-value as prob, all as an array. Small values of prob show that
    the sample is significantly different from the model. Note that the test is slightly distribution-
    dependent, so prob is only an estimate.
    """
    d1=prob=0.0
    n1=len(x1)
    r1=dum=dumm=0.0
    ks = KSdist()
    for j in range(n1):
        [fa,fb,fc,fd] = quadct(x1[j],y1[j],x1,y1)
        [ga,gb,gc,gd] = quadvl(x1[j],y1[j])
        if(fa>ga): fa += 1.0/n1
        if(fb>gb): fb += 1.0/n1
        if(fc>gc): fc += 1.0/n1
        if(fd>gd): fd += 1.0/n1
        d1 = max(d1,abs(fa-ga))
        d1 = max(d1,abs(fb-gb))
        d1 = max(d1,abs(fc-gc))
        d1 = max(d1,abs(fd-gd))
    [r1,dum,dumm] = pearsn(x1,y1)
    sqen = math.sqrt(n1)
    rr=math.sqrt(1.0-r1*r1)
    prob = ks.qks(d1*sqen/(1.0+rr*(0.25-0.75/sqen)))
    
    return [d1,prob]


def quadct(x,y,xx,yy):
    """Given an origin (x,y), and an array of nn points with coordinates
    xx[0..nn-1] and yy[0..nn-1], count how many of them are in each
    quadrant around the origin, and return the normalized fractions.
    Quadrants are labeled alphabetically, a counterclockwise from the 
    upper right. Used by ks2d1s and ks2d2s.
    """
    na=nb=nc=nd=0
    nn = len(xx)
    for k in range(nn):
        if(yy[k]==y and xx[k]==x): continue
        if(yy[k]>y):
            if(xx[k]>x): na += 1
            else: nb += 1
        else:
            if(xx[k]>x): nd += 1
            else: nc += 1
    ff=1.0/nn
    return [ff*na,ff*nb,ff*nc,ff*nd]


# In[79]:

def quadvl(x,y):
    """This is a sample of a user-supplied routine to be used with ks2d1s.
    In this case, the model distribution is uniform inside the square.
    """
    qa=min(2.0,max(0.0,1.0-x))
    qb=min(2.0,max(0.0,1.0-y))
    qc=min(2.0,max(0.0,x+1.0))
    qd=min(2.0,max(0.0,y+1.0))
    fa=0.25*qa*qb
    fb=0.25*qb*qc
    fc=0.25*qc*qd
    fd=0.25*qd*qa
    
    return [fa,fb,fc,fd]


# In[80]:

def ks2d2s(x1,y1,x2,y2):
    """Two-dimensional Kolmogorow-Smirnov test on two sampls. Given the x
    and y coordinates of the first sample as n1 values in arrays x1[0..n1-1]
    and y1[0..n1-1], and likewise for the second sample, n2 values in arrays
    x2 and y2, this routine returns the two-dimensional, two-sample K-S 
    statistic as d, and its p-value as prob, all as an array. Small values 
    of prob show that the two samples are significantly different. Note that
    the test is slightly distribution-dependent, so prob is only an estimate.
    """
    n1=len(x1)
    n2=len(x2)
    r1=r2=rr=dum=dumm=0.0
    ks = KSdist()
    d1=0.0
    for j in range(n1):
        [fa,fb,fc,fd] = quadct(x1[j],y1[j],x1,y1)
        [ga,gb,gc,gd] = quadct(x1[j],y1[j],x2,y2)
        if(fa>ga): fa += 1.0/n1
        if(fb>gb): fb += 1.0/n1
        if(fc>gc): fc += 1.0/n1
        if(fd>gd): fd += 1.0/n1
        d1 = max(d1,abs(fa-ga))
        d1 = max(d1,abs(fb-gb))
        d1 = max(d1,abs(fc-gc))
        d1 = max(d1,abs(fd-gd))
    d2=0.0
    for j in range(n2):
        [fa,fb,fc,fd] = quadct(x2[j],y2[j],x1,y1)
        [ga,gb,gc,gd] = quadct(x2[j],y2[j],x2,y2)
        if(ga>fa): ga += 1.0/n1
        if(gb>fb): gb += 1.0/n1
        if(gc>fc): gc += 1.0/n1
        if(gd>fd): gd += 1.0/n1
        d2 = max(d2,abs(fa-ga))
        d2 = max(d2,abs(fb-gb))
        d2 = max(d2,abs(fc-gc))
        d2 = max(d2,abs(fd-gd))
    d=0.5*(d1+d2)
    sqen=math.sqrt(n1*n2/float(n1+n2))
    [r1,dum,dumm] = pearsn(x1,y1)
    [r2,dum,dumm] = pearsn(x2,y2)
    rr = math.sqrt(1.0-0.5*(r1*r1+r2*r2))
    prob=ks.qks(d*sqen/(1.0+rr*(0.25-0.75/sqen)))
    return (rr, prob) 

# # Statistical Description of Data - Scientific Computing II - Alex Hernandez 

# ## Basic Statistical Tools

import matplotlib, math, sys, numpy

# ## Special Functions Code
# 
# 
def beta(z,w):
    """Returns the value of the beta function B(z,w)"""
    return math.exp(gammln(z)+gammln(w)-gammlb(z+w))

class Gauleg18(object):
    ngau = 18
    y = [ 0.0021695375159141994, 
          0.011413521097787704,0.027972308950302116, 0.051727015600492421,
          0.082502225484340941, 0.12007019910960293, 0.16415283300752470,
          0.21442376986779355, 0.27051082840644336, 0.33199876341447887,
          0.39843234186401943, 0.46921971407375483, 0.54413605556657973,
          0.62232745288031077, 0.70331500465597174, 0.78649910768313447,
          0.87126389619061517, 0.95698180152629142]
    w = [ 0.0055657196642445571, 
          0.012915947284065419, 0.020181515297735382, 0.027298621498568734,
          0.034213810770299537, 0.040875750923643261, 0.047235083490265582,
          0.053244713977759692, 0.058860144245324798, 0.064039797355015485,
          0.068745323835736408, 0.072941885005653087, 0.076598410645870640,
          0.079687828912071670, 0.082187266704339706, 0.084078218979661945,
          0.085346685739338721, 0.085983275670394821]


class Beta(Gauleg18):
    """ Obhect for incomplete beta function. Gauleg 18 provide coefficients for Gauss-Lgendre
    qaudrature.
    """
    SWITCH=3000
    def __init__(self):
        self.EPS = sys.float_info[8]
        self.FPMIN = sys.float_info[3]/self.EPS
        
    def betai(self, a,b,x):
        """ Return incomplete beta funciton I_x(a,b) for a positive a and b, and x between 0 and 1.
        """
        if(a<=0.0 or b<=0.0): raise Exception("Bad a or b in routine betai")
        if(x<0.0 or x > 1.0): raise Exception("Bad x in rountine betai")
        if(x == 0.0 or x == 1.0): return x
        if(a > self.SWITCH and b > self.SWITCH): return betaiapprox(a,b,x)
        bt=math.exp(gammln(a+b)-gammln(a)-gammln(b)+a*math.log(x)+b*math.log(1.0-x))
        if(x<(a+1.0)/(a+b+2.0)): return bt*self.betacf(a,b,x)/a
        else: return 1.0-bt*self.betacf(b,a,1.0-x)/b
        
    def betacf(self,a,b,x):
        """Evaluates continued fraction for incomplete beta funciton by modified Lentz's
        method. User should not call directly.
        """
        qab=a+b
        qap=a+1.0
        qam=a-1.0
        c=1.0
        d=1.0-qab*x/qap
        if( abs(d) < self.FPMIN): d=self.FPMIN
        d=1.0/d
        h=d
        for m in range(1,10000):
            m2=2*m
            aa=m*(b-m)*x/((qam+m2)*(a+m2))
            d=1.0+aa*d
            if(abs(d)<self.FPMIN): d=self.FPMIN
            c=1.0+aa/c
            if(abs(c)<self.FPMIN): c=self.FPMIN
            d=1.0/d
            h *= d*c
            aa = -(a+m)*(qab+m)*x/((a+m2)*(qap+m2))
            d=1.0+aa*d
            if(abs(d)<self.FPMIN): d=self.FPMIN
            c=1.0+aa/c
            if(abs(c)<self.FPMIN): c=self.FPMIN
            d=1.0/d
            dl = d*c
            h *= dl
            if(abs(dl-1.0)<=self.EPS): break
        return h
    
    def betaiapprox(self,a,b,x):
        """ Incomplete beta by quadrature. Reutrns I_x(a,b). User should not call directly.
        """
        a1=a-1.0
        b1=b-1.0
        mu=a/(a+b)
        lnmu=math.log(mu)
        lnmuc=math.log(1.0-mu)
        t = math.sqrt(a*b/( ((a+b)**2)*(a+b+1.0)))
        if(x> a/(a+b)):
            if(x>=1.0): return 1.0
            xu = min(1.0, max(mu+10.0*t, x+5.0*t))
        else:
            if(x<=0.0): return 0.0
            xu = max(0.0, min(mu-10.0*t, x-5.0*t))
        sm = 0.0
        for j in range(0,18):
            t = x + (xu-x)*self.y[j]
            sm += w[j]*math.exp(a1*(log(t)-lnmu)+b1*(log(1-t)-lnmuc))
        ans = sum*(xu-x)*math.exp(a1*lnmu-gammln(a)+b1*lnmuc-gammln(b)+gammln(a+b))
        if(ans>0.0): return 1.0-ans
        else: return -ans
        
    def invbetai(self,p,a,b):
        """Inverse of incomplete beta funciton. Returns x such that I_x(a,b) = p 
        for argument p between 0 and 1.
        """
        pp=t=u=err=x=al=h=w=afac=0.0
        j = 0
        EPS = 1.0e-8
        a1=a-1.0
        b1=b-1.0
        if(p <= 0.0): return 0.0
        elif(p >= 1.0): return 1.0
        elif(a >= 1.0 and b >= 1.0):
            if(p<0.5): pp = p
            else: pp = 1.0 - p
            t = math.sqrt(-2.0*math.log(pp))
            x = (2.30753+t+0.27061)/(1.0+t*(0.99229+t*0.04481))-t
            if(p<0.5): x = -x
            al = ( x**2 -3.0)/6.0
            h = 2.0/(1.0/(2.0*a-1.0)+1.0/(2.0*b-1.0))
            w = (x*math.sqrt(al+h)/h)-(1.0/(2.0*b-1)-1.0/(2.0*a-1.0))*(al+5.0/6.0-2.0/(3.0*h))
            x = a/(a+b*math.exp(2.0*w))
        else:
            lna = math.log(a/(a+b))
            lnb = math.log(b/(a+b))
            t = math.exp(a*lna)/a
            u = math.exp(b*lnb)/b
            w = t + u
            if(p<(t/w)): x = (a*w*p)**(1.0/a)
            else: x = 1.0 - (b*w*(1.0-p))**(1.0/b)
        afac = -gammln(a)-gammln(b)+gammln(a+b)
        for j in range(0,10):
            if(x ==0.0 or x == 1.0): return x
            err = self.betai(a,b,x)-p
            t = math.exp(a1*math.log(x)+b1*math.log(1.0-x)+afac)
            u = err/t
            t = u/(1.0-0.5*min(1.0, u*(a1/x - b1/(1.0-x))))
            x -= t
            if(x<=0.0): x = 0.5*(x+t)
            if(x>=1.0): x = 0.5*(x+t+1.0)
            if( abs(t) < EPS*x and j>0): break
        return x


# ## Satisitical Functions Objects
# ### Kolmogorov-Smirnov Distribution

def invxlogx(y):
    """For negative y, 0>y>-e^(-1), return x such that y = xlog(x).
    The value returned is always the smaller of the two roots and
    is in the range 0 < x < e^(-1).
    """
    ooe = 0.33678879441171442322
    u=0.0
    t=0.0
    to=0.0
    if(y>=0.0 or y<= -ooe): raise Exception("no such inverse value")
    if(y<-0.2): u = math.log(ooe-math.sqrt(2.0*ooe*(y+ooe)))
    else: u = -10.0
    t=(math.log(y/u)-u)*(u/(1.0+u))
    u+=t
    if(t<1.0e-8 and abs(t+to)<0.01*abs(t)):
        to = t
        while(abs(t/u)>1.0e-15):
            t=(math.log(y/u)-u)*(u/(1.0+u))
            u+=t
            if(t<1.0e-8 and abs(t+to)<0.01*abs(t)): break
            to=t
    return math.exp(u)


# In[430]:

class KSdist(object):
    """Kolmogorow-Smirnov culative distribution function and their inverses."""
    def pks(self,z):
        """Return cumulative distribution function"""
        if(z<0.0): raise Exception("bad z in KSdist")
        if(z==0.0): return 0.0
        if(z<1.18):
            y = math.exp(-1.23370055013616983/(z**2))
            return 2.25675833419102515*math.sqrt(-math.log(y))                     *(y+(y**9)+(y**25)+(y*49))
        else:
            x = math.exp(-2.0*(z**2))
            return 1.0-2.0*(x-(x**4)+(x**9))
        
    def qks(self,z):
        """Return complementary cumulative distribution function."""
        if(z<0.0): raise Exception("bad z in KSdist")
        if(z==0.0): return 1.0
        if(z<1.18): return 1.0-self.pks(z)
        x = math.exp(-2.0*(z**2))
        return 2.0*(x-(x**4)+(x**9))
    
    def invqks(self,q):
        """Return inverse of the complementary cumulative distribution function."""
        x=0.0
        y=0.0
        if(q<=0.0 or q>1.0): raise Exception("bad q in KSdist")
        if(q==1.0): return 0.0
        if(q>0.3):
            f=-0.392699081698724155*((1.0-q)**2)
            y=invxlogx(f)
            yp = y
            logy = math.log(y)
            ff = f/((1.0+(y**4)+(y**12))**2)
            u = (y*logy-ff)/(1.0+logy)
            t=u/max(0.5,1.0-0.5*u/(y*(1.0+logy)))
            while( abs(t/y)>1.0e-15):
                f=-0.392699081698724155*((1.0-q)**2)
                y=invxlogx(f)
                yp = y
                logy = math.log(y)
                ff = f/((1.0+(y**4)+(y**12))**2)
                u = (y*logy-ff)/(1.0+logy)
                t=u/max(0.5,1.0-0.5*u/(y*(1.0+logy)))
            return 1.57079632679489662/math.sqrt(-math.log(y))
        else:
            x=0.03
            xp=x
            x = 0.5*q+(x**4)-(x**9)
            if(x>0.06): x+= (x**16)-(x**25)
            while( abs((xp-x)/x)>1.0e-15 ):
                xp=x
                x = 0.5*q+(x**4)-(x**9)
                if(x>0.06): x+= (x**16)-(x**25)
            return math.sqrt(-0.5*math.log(x))
    
    def invpks(self,p):
        """Return inverse of the cumulative distribution funciton."""
        return invqks(1.0-p)
            

