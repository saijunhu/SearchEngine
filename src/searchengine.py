from urllib.request import urlopen,Request,urljoin
import certifi
import re
from bs4 import BeautifulSoup
import sqlite3
import src.nn
mynet = src.nn.searchnet('nn.db')


# Create a list of words to ignore
ignorewords={'the':1, 'of':1, 'to':1, 'and':1, 'a':1, 'in':1, 'is':1, 'it':1}

class crawler:
    # initial the crawler with the name of database
    def __init__(self,dbname):
        self.con = sqlite3.connect(dbname)

    def __del__(self):
        self.con.close()

    def db_commit(self):
        self.con.commit()

    # auxilliary function for getting an entry id and adding it if it's not present
    def get_entry_id(self, table, field, value,createnew=True):
        cursor = self.con.cursor()
        cursor.execute(
            "select rowid from %s where %s='%s'" % (table, field, value))
        res = cursor.fetchone()
        if res is None:
            cur = self.con.execute(
                "insert into %s (%s) values ('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]


            # Index an individual page
    def add_to_index(self, url, soup):
        if self.is_indexed(url): return
        print('Indexing %s' % url)

        # get the individual words
        text = self.get_text_only(soup)
        words = self.separate_words(text)

        #get the url id
        urlid = self.get_entry_id('urllist', 'url',url)

        # link each word to this url
        cursor = self.con.cursor()
        for i in range(len(words)):
            word=words[i]
            if word in ignorewords: continue
            wordid = self.get_entry_id('wordlist','word',word)
            cursor.execute("insert into wordlocation(urlid,wordid,location) values (%d,%d,%d)" % (urlid, wordid, i))


    # Extract the text form an html page(no tags),
    # this place is the bug buffled me a fully day ,now is ok
    def get_text_only(self, soup):
        v = soup.string
        if v is None:
            c = soup.contents
            resulttext = ''
            for t in c:
                s = t.string

                if type(t) != "BeautifulSoup.Declaration":
                    subtext = self.get_text_only(t)
                    resulttext += subtext + '\n'
            return resulttext
        else:
            return v.strip()
        # return soup.get_text()

    # Separate the  words by any no-whitespace character
    def separate_words(self, text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s!='']


    # Return true if this url is already indexed
    def is_indexed(self, url):
        cursor = self.con.cursor()
        u=cursor.execute("select rowid from urllist WHERE url='%s' " % url).fetchone()
        if u is not None:
            # check if it has actually been crawled
            v = cursor.execute("select * from wordlocation WHERE urlid= '%s' " % u[0]).fetchone()
            if v is not None: return True
        return False

    # Add a link between two pages
    def add_link_ref(self, urlFrom, urlTo, linkText):
        words = self.separate_words(linkText)
        fromid = self.get_entry_id('urllist', 'url', urlFrom)
        toid = self.get_entry_id('urllist', 'url', urlTo)
        if fromid == toid: return
        cur = self.con.execute("insert into link(fromid,toid) values (%d,%d)" % (fromid, toid))
        linkid = cur.lastrowid
        for word in words:
            if word in ignorewords: continue
            wordid = self.get_entry_id('wordlist', 'word', word)
            self.con.execute("insert into linkwords(linkid,wordid) values (%d,%d)" % (linkid, wordid))

    # Starting with a list of pages, do a breadth first search
    #  to the given depth, indexing pages as we go.
    def crawl(self, pages, depth=2):
        cursor = self.con.cursor()
        for i in range(depth):
            newpages = {}
            for page in pages:
                try:
                    c =urlopen(page)
                except:
                    print( 'could not open the page %s' % page)
                    continue
                soup = BeautifulSoup(c.read())
                self.add_to_index(page, soup)
                links = soup.find_all('a')
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url = urljoin(page,link['href'])
                        if url.find("'") != -1: continue
                        url = url.split('#')[0] #remove location portion
                        if url[0:4] == 'http' and not self.is_indexed(url):
                            newpages[url]=1
                        lineText=self.get_text_only(link)
                        self.add_link_ref(page, url, lineText)
                self.db_commit()
            pages=newpages


    # Create the database tables
    def create_index_tables(self):
        cursor = self.con.cursor()
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid,wordid,location)')
        self.con.execute('create table link(fromid integer,toid integer)')
        self.con.execute('create table linkwords(wordid,linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordurlidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.db_commit()

    # use PageRank alg,and precompute it , run it every you run crawl
    def calculate_pagerank(self,iterations=20):
        # clear out the current pagerank tables
        self.con.execute('drop table if exists pagerank')
        self.con.execute('create table pagerank(urlid primary key, score)')

        # initialize every url with a pagerank of 1(it doesn't matter)
        for (urlid,) in self.con.execute('select rowid from urllist'):
            self.con.execute('insert into pagerank(urlid,score) values (%d,1.0)' % urlid)
        self.db_commit()

        for i in range(iterations):
            print('Iteration %d' % i)
            for(urlid,) in self.con.execute('select rowid from urllist'):
                pr=0.15 # the pagerank alg constant
                # loop throufh all the pages that link this one
                for(linker,) in self.con.execute('select distinct fromid from link where toid=%d' % urlid):
                    # get the pagerank of the linker
                    linkingpr = self.con.execute('select score from pagerank where urlid=%d' % linker).fetchone()[0]
                    # get the total number of links form the linker
                    linkingcount =self.con.execute('select count(*) from link where fromid=%d' % linker).fetchone()[0]
                    # calculate
                    pr+=0.85*(linkingpr/linkingcount)
                    self.con.execute('update pagerank set score=%f where urlid=%d' % (pr, urlid))
                    self.db_commit()




class seacher:
    def __init__(self, dbname):
        self.con = sqlite3.connect(dbname)

    def __del__(self):
        self.con.close()

    def get_match_rows(self, q):
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        # split the words by spaces
        words = q.split(' ')
        tablenumber = 0

        for word in words:
            # get the word id
            wordrow = self.con.execute("select rowid from wordlist where word \
                    = '%s'" % word).fetchone()
            # TODO the folling code can't understand
            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist += ' and '
                    clauselist += 'w%d.urlid = w%d.urlid and ' % (tablenumber -
                                                                  1, tablenumber)
                fieldlist += ',w%d.location' % tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid = %d' % (tablenumber, wordid)
                tablenumber += 1

        # create the query from the separate parts
        fullquery = 'select %s from %s where %s ' % (fieldlist, tablelist,
                                                     clauselist)
        cur = self.con.execute(fullquery)
        rows = [row for row in cur]
        return rows, wordids

    def get_sorted_list(self, rows, wordids):
        totalscores = dict([(row[0],0) for row in rows])
        # this is where you will later put the scoring functions
        # this func is amazing !!!!!, good example for weight sth
        weights=[(0.6, self.frequencys_score(rows)),(0.2, self.locations_scores(rows)),
                 (0.2,self.distance_score(rows)), (0.3, self.inbund_link_store(rows)),
                 (0.6, self.pagerank_score(rows)), (0.5, self.linktext_score(rows,wordids)),
                 (0.6, self.nn_score(rows,wordids)) ]
        for (weight,scores) in weights:
            for url in totalscores:
                totalscores[url]+=weight*scores[url]

        return totalscores

    def get_url_name(self, urlid):
        return self.con.execute("select url from urllist where rowid=%d " % urlid).fetchone()[0]


    # normalize the scores for diff weight function
    def frequencys_score(self, rows):
        counts = dict([ (row[0],0) for row in rows ])
        for row in rows:
            counts[row[0]]+=1
        return self.normalize_scores(counts)


    # Score method one: use the word frequencys to appraise the page priority
    def normalize_scores(self, scores, smallisbetter=False):
        vsmall=0.00001 # avoid division by zero errors
        if smallisbetter:
            minscore = min(scores.values())
            return dict([(u, float(minscore)/max(vsmall,l)) for (u,l) in scores.items()])
        else:
            maxscore = max(scores.values())
            return dict([(u, float(c) / maxscore) for (u, c) in scores.items()])

    # Score method two: use the word location in docuement to evaluate
    def locations_scores(self, rows):
        locations = dict([(row[0], 10000) for row in rows])
        for row in rows:
            # if multi keywords, the location just simply add
            loc = sum(row[1:])
            locations[row[0]] = min(loc, locations[row[0]])
        return self.normalize_scores(locations, smallisbetter=True)

    # Score method three: focus the distance between keywords
    def distance_score(self, rows):
        # if only one keyword query
        if len(rows[0]) <= 2:
            return dict([(row[0],1.0) for row in rows])
        # else initialize the dict
        mindistance = dict([(row[0],100000) for row in rows])

        for row in rows:
            dist = sum([abs(row[i]-row[i-1]) for i in range(2,len(row))])
            mindistance[row[0]]=min(dist,mindistance[row[0]])
        return self.normalize_scores(mindistance,smallisbetter=True)

    # Score method four: use the inbound links count
    def inbund_link_store(self, rows):
        uniqueurls= set([row[0] for row in rows])
        inboundcount = dict([(u, self.con.execute('select count(*) from link where toid=%d' % u).fetchone()[0])\
                             for u in uniqueurls])
        return self.normalize_scores(inboundcount,smallisbetter=False)

    # Score method five : use the PageRank Score
    def pagerank_score(self, rows):
        pageranks= dict([(row[0],self.con.execute('select score from pagerank where urlid=%d' % row[0]
                                                ).fetchone()[0]) for row in rows])
        return self.normalize_scores(pageranks)


    # Score method six: use the linktext
    def linktext_score(self, rows, wordids):
        linkscores=dict([(row[0],0) for row in rows])
        for wordid in wordids:
            cur = self.con.execute('select link.fromid,link.toid from linkwords,link where wordid=%d and linkwords.linkid=link.rowid' % wordid)
            for (fromid, toid) in cur:
                if toid in linkscores:
                    pr=self.con.execute('select score from pagerank where urlid=%d' % fromid).fetchone()[0]
                    linkscores[toid]+=pr
        return self.normalize_scores(linkscores)

    # Score method seven: use the neutral network
    def nn_score(self, rows, wordids):
        urlids = [urlid for urlid in set([row[0] for row in rows])]
        mynet.generate_hidden_node(wordids,urlids) # try
        nnres = mynet.get_result(wordids,urlids)
        scores = dict([(urlids[i],nnres[i]) for i in range(len(urlids))])
        return self.normalize_scores(scores)

    def query(self, q,resultsize=10):
        rows, wordids = self.get_match_rows(q)
        scores = self.get_sorted_list(rows,wordids)
        rankedscores = sorted([(score, url) for (url, score) in scores.items()], reverse=True)
        for (score, urlid) in rankedscores[0:resultsize]:
            print('%f\t%s' % (score, self.get_url_name(urlid)))
        return wordids,[r[1] for r in rankedscores]





# pagelist = ['http://www.paulgraham.com/articles.html']
pagelist = ['http://www.chinadaily.com.cn']
crawler = crawler('searchengine.db')
# crawler.create_index_tables()

e= seacher('searchengine.db')
e.query('china usa')