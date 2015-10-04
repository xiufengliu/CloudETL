# A program for generating example data for "pygrametl: A Powerful Programming 
# Framework for Extract--Transform--Load Programmers"

#  
#  Copyright (c) 2009 Christian Thomsen (chr@cs.aau.dk)
#  
#  This file is free software: you may copy, redistribute and/or modify it  
#  under the terms of the GNU General Public License version 2 
#  as published by the Free Software Foundation.
#  
#  This file is distributed in the hope that it will be useful, but  
#  WITHOUT ANY WARRANTY; without even the implied warranty of  
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU  
#  General Public License for more details.  
#  
#  You should have received a copy of the GNU General Public License  
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.  
#  

import random, sys, os, getopt, math
from sets import Set
from datetime import timedelta, datetime
import io



def get_map_num():    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)

    for o, a in opts:
        if o in ("-h", "--help"):
            print __doc__
            sys.exit(0)
    return (int(args[0]), int(args[1]))




toplevels = 999
domains = 9999
pages = 99999

months = 4000
startyear = 1980
tests = 5

def gen_int_byrange(st, ed):
    rnd = random.Random()
    return rnd.randint(st, ed)


def random_date(start, end):
    d1 = datetime.strptime(start, '%Y-%m-%d')
    d2 = datetime.strptime(end, '%Y-%m-%d')
    delta = d2 - d1
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    d = (d1 + timedelta(seconds=random_second))
    return d.strftime("%Y-%m-%d")


def get_next_date(thedate, delta):
    d1 = datetime.strptime(thedate, '%Y-%m-%d')
    d = (d1 + timedelta(seconds=delta * 24 * 60 * 60))
    return d.strftime("%Y-%m-%d")


def gen_date():
    return random_date('1900-01-01', '2012-10-01')



def generateurls():
    toplevellist = ["tl%02d" % (i,) for i in range(toplevels)]
    domainlist = ["domain%03d" % (i,) for i in range(domains)] # in each toplevel
    pagelist = ["page%03d.html" % (i,) for i in range(pages)]# in each domain

    for t in toplevellist:
        for d in domainlist:
            for p in pagelist:
                yield "http://www.%s.%s/%s" % (d, t, p)


#localfile         url                                    serverversion    size    downloaddate    lastmoddate
#24612006.tmp    http://www.domain000.tl13/page005.html  PowerServer/1.0   8441    2005-08-01      2005-07-20

def generate_pageinfo2(outfile, datasize, paths, row_nums=9999, chgprob=30, scdnum=4):
    rownum = math.trunc(datasize*1024.0*1024*1024/97.0)  #11302545 
    servers = ['PowerServer/1.0', 'PowerServer/2.0', 'SuperServer/3.0']

    print "Generate %d lines of page" % (rownum,)
    count = 0
    for t in range(toplevels):
        for d in range(domains):
            for p in range(pages):
                localfile = "%08d.tmp"  % (count,)
                url = "http://www.domain%03d.tl%04d/page%05d.html" % (d, t, p)
                serverversion = servers[gen_int_byrange(0,2)]
                size = gen_int_byrange(100, 999)
                downloaddate = gen_date()
                lastmoddate = gen_date() 
                line = [localfile, url, serverversion, size, downloaddate, lastmoddate]
                writeline(outfile, line)
                count = count + 1
                if count>=rownum:
                    return		
                
                if chgprob>0:
                    prob = gen_int_byrange(0,100)
                    if prob<chgprob:
                        scdnum = gen_int_byrange(1, scdnum)
                        prevmoddate = lastmoddate
                        for i in range(scdnum):
                            lastmoddate = get_next_date(prevmoddate, gen_int_byrange(1, 5))
                            size = gen_int_byrange(100, 999)
                            serverversion = servers[gen_int_byrange(0,2)]
                            line = [localfile, url, serverversion, size, downloaddate, lastmoddate]
                            writeline(outfile, line)
                            count = count + 1
                            if count>=rownum:
                                return
                            prevmoddate = lastmoddate





def generate_pageinfo(outfile, size, paths, row_nums, chgprob):
    nrows = math.trunc(size*1024.0*1024*1024/95.0)  #11302545 

    servers = ['PowerServer/1.0', 'PowerServer/2.0', 'SuperServer/3.0']
    cache = {}
    rnd = random.Random()
    rnd.seed(1) # We want to be able to recreate results

    i = 0
    year = startyear
    totalrows = 0

    for month in range(1, months + 1):
        urls = generateurls()
        if month > 1 and month % 12 == 1: # It is January in a new year
            year += 1  
        downloaddate = "%d-%02d-01" % (year,  month % 12 == 0 and 12 or month % 12)
        for url in urls:
            i += 1
            localfile = "%08d.tmp" % (i,)
            # With probability 100 - changeprob, the page wasn't changed
            tmp = rnd.randint(1,100)
            if tmp <= chgprob and url in cache:
                line = cache.get(url)
                line[0] = localfile
                line[4] = downloaddate
                if totalrows<nrows:
                    writeline(outfile, line)
                    totalrows = totalrows + 1
                    continue
                else:
                    print 'Generate %d pages' % totalrows
                    return totalrows
            # Else simulate some changes...

            # A given domain is always using the same server in this example.
            # We extract the number in the domain and use that to determine the
            # server.
            tmp = int(url.split(".tl")[0].split("domain")[1])
            server = servers[tmp % len(servers)]
            size = rnd.randint(1000, 9000)

            tmp = rnd.randint(2,28)
            if month % 12 == 1:
                lastmoddate = "%d-12-%02d" % (year - 1, tmp)
            elif month % 12 == 0:
                lastmoddate = "%d-11-%02d" % (year, tmp)
            else:
                lastmoddate = "%d-%02d-%02d" % (year, month % 12 - 1, tmp)
            line = [localfile, url, server, size, downloaddate, lastmoddate]
            cache[url] = line
            if totalrows<nrows:
                writeline(outfile, line)
                totalrows = totalrows + 1
            else:
                print 'Generate %d pages' % totalrows
                return totalrows
    print 'Generate %d pages' % totalrows
    return totalrows



def generate_tester(outfile, size, paths, row_nums, chgprob):
    #nrows = math.trunc(size*1024.0*1024*1024/8.0)  #11302545 
    testlist = ['Test%03d' % (i,) for i in range(999)]
    nrows = 0
    for test in testlist:
        line = (test,)
        writeline(outfile, line)
        nrows += 1
    return nrows

def generate_testresults(outfile, size, paths, row_nums, chgprob):
    nrows = math.trunc(size*1024.0*1024*1024/85.0)  #11302545 

    pageinfo = open(paths[0], 'r', 16384)
    testinfo = open(paths[1], 'r', 16384)
    rnd = random.Random()
    for i in range(nrows):
        pagelength = 95*rnd.randint(1, row_nums[0]-1)
        testlength = 8*rnd.randint(1, row_nums[1]-1)
        pageinfo.seek(pagelength)
        pageline = pageinfo.readline()
        testinfo.seek(testlength)
        testline = testinfo.readline()
        #print pageline
        if pageline==None or pageline=='':
            print pagelength
            print pageline
            return
        logfields = pageline.split('\t')

        line = (logfields[0], logfields[1], logfields[5].rstrip('\n'), logfields[4], testline.rstrip('\n'), rnd.randint(10, 99))
        writeline(outfile, line)
    pageinfo.close()
    testinfo.close()
    print 'Generate %d testresults' % nrows

def writeline(outfile, fields):
    outfile.write("\t".join([str(f) for f in fields]))
    outfile.write("\n")



FUNS = (generate_pageinfo2, generate_tester, generate_testresults)
def gen_data(indices, sizes, saved_path, chgprob):
    fpattern = "%s/%s.csv" 
    row_nums = []
    paths = []
    for i in indices:
        path = fpattern % (saved_path, TABLE_NAMES[i])
        func = FUNS[i]
        f = open(path, 'w',  16384)
        row_num = func(f, sizes[i], paths, row_nums, chgprob)
        f.close()
        row_nums.append(row_num)
        paths.append(path)


TABLE_NAMES=('pages', 'tests', 'testresults', 'all')
def main():
    msg = "; ".join(list(str(i+1) +"."+TABLE_NAMES[i] for i in range(len(TABLE_NAMES))))
    path = '/tmp'
    while True:
        try:
            sizes = {}
            indices = []
            chgprop = 0
            selected = int(raw_input('Generate data for (%s):'% msg))-1
            if selected!=3:
                indices.append(selected)
                sizes[selected] = float(raw_input('Please input the size (GB) for %s:' %TABLE_NAMES[selected]))
                if selected==0:
                    chgprop = int(raw_input('Please input %s SCD change prob (0-100, 0:no change):'%TABLE_NAMES[selected]))
            elif selected==3:
                n = 0
                while n<len(TABLE_NAMES)-1:
                    indices.append(n)
                    sizes[selected] = float(raw_input('Please input the size (GB) for %s:' %TABLE_NAMES[n]))
                    if n==0:
                        chgprop = int(raw_input('Please input %s SCD change prob (0-100, 0:no change):'%TABLE_NAMES[selected]))
                    n += 1
            path = raw_input('Please input the path for saving data (default: /tmp):')
            if not os.path.isdir(path):
                path = '/tmp'
            gen_data(indices, sizes, path, chgprop)
            if raw_input('Do you continue generating data (y/n):')!='y':
                break
        except:
            print "for help use --help"
            sys.exit(2)



if __name__ == "__main__":
    main()