from urllib.request import urlopen,Request,urljoin
import certifi
import re
from bs4 import BeautifulSoup
import sqlite3


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
    #TODO this place is the bug where
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
        pass

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


# pagelist = ['http://www.paulgraham.com/articles.html']
# crawler = crawler('searchengine.db')
# # crawler.create_index_tables()
#
# crawler.crawl(pagelist)

e= seacher('searchengine.db')
print(e.get_match_rows('java'))