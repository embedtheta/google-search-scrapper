import requests
from bs4 import BeautifulSoup
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
from fake_useragent import UserAgent
import random
from time import sleep, time
import tldextract
import pandas as pd
import whois
import re
import datetime
from dateutil.relativedelta import relativedelta
import os
import pymongo
import pandas as pd
import itertools as ist

all_intitle_switch = True

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
dblist = myclient.list_database_names()
mydb = myclient["mydatabase"]
if "mydatabase" in dblist:
    print("The database exists.")

mycol = mydb["analysis"]
mycol_2 = mydb["forumlinks"]
mycol_3 = mydb["searchsugession"]
if all_intitle_switch:
    mycol_4 = mydb["allintitle"]
collist = mydb.list_collection_names()
if "analysis" in collist:
    print("The collection exists.")

filename = "Keywords.xlsx"
ua = UserAgent()
headers = {
    "User-Agent": ua.random
}
social_share = ["facebook", "linkedin", "twitter", "pinterest", "youtube", "dailymotion"]
qa_site = ["quora", "reddit", "answer"]
affiliate = ["best", "review", "reviews"]
forum = ["forum", "thread", "showthread", "discussion","viewtopic","topic"]
regexes_forum = [re.compile('\b[0-9f0-9]\b')]
search_suggestion_result = []
forum_links = []


class MultiThreadScraper:

    def __init__(self, base_url):

        self.base_url = base_url
        # self.root_url = '{}://{}'.format(urlparse(self.base_url).scheme, urlparse(self.base_url).netloc)
        # self.pool = ThreadPoolExecutor(max_workers=50)
        self.scraped_pages = set([])
        self.to_crawl = Queue()
        self.scraped_pages_allintitle = set([])
        self.to_crawl_allintitle = Queue()
        self.to_crawl_remain = set([])
        self.to_crawl_remain_allintitle = set([])
        # self.to_crawl.put(self.base_url)
        # self.analysis_data = []

    def parse_links(self, html, keyword):
        links_title = []
        total_count = []
        registration_date_latest = []
        social_count = 0
        q_a_count = 0
        forum_count = 0
        affiliate_count = 0
        other_count = 0
        root_count = 0
        page_count = 0
        below_1 = 0
        below_2 = 0
        more_than = 0
        registration_date = []
        soup = BeautifulSoup(html, "html.parser")
        try:
            snippet = soup.select_one(".ifM9O").find('h2', class_="bNg8Rb")
            if snippet:
                snipper_string = "Yes"
            else:
                snipper_string = "No"
        except Exception:
            snipper_string = "No"

        for title in soup.find_all("p", class_='nVcaUb'):
            try:
                search_suggestion = title.get_text()
                if search_suggestion.startswith("best") or search_suggestion.endswith("review") or \
                        search_suggestion.endswith("reviews"):
                    columns = ["Search Keywords", "Search Suggestion Result"]
                    suggestion_result = [keyword, search_suggestion]
                    # Create a zip object from two lists
                    searchObj = zip(columns,  suggestion_result)
                    # Create a dictionary from zip object
                    seachofWords = dict(searchObj)
                    # print(registration_date)
                    x = mycol_3.insert_one(seachofWords)
            except Exception:
                pass
        for item in soup.find_all("div", class_='r'):
            rows = item.find('a')
            try:
                title = rows.find('h3').get_text()
            except Exception:
                title = ""
            try:
                url = rows.get('href')
            except Exception:
                url = ""
            links_title.append([title, url])

        for i in range(len(links_title)):
            ext = tldextract.extract(links_title[i][1])
            url = ext.domain + "." + ext.suffix
            try:
                w = whois.whois(url)
                try:
                    start_date = w.creation_date[0]
                    registration_date.extend([url, datetime.datetime.strptime(str(start_date), '%Y-%m-%d %H:%M:%S').date().strftime("%m/%d/%Y")])
                    registration_date_latest.append(datetime.datetime.strptime(str(start_date), '%Y-%m-%d %H:%M:%S').
                                                    date())
                except:
                    start_date = w.creation_date
                    registration_date.extend([url, datetime.datetime.strptime(str(start_date), '%Y-%m-%d %H:%M:%S').date().strftime("%m/%d/%Y")])
                    registration_date_latest.append(datetime.datetime.strptime(str(start_date), '%Y-%m-%d %H:%M:%S').
                                                    date())
            except:
                creation_date = ''
                registration_date.extend([url, creation_date])

            try:
                now = datetime.datetime.now()
                start = datetime.datetime.strptime(str(start_date), '%Y-%m-%d %H:%M:%S').date()
                ends = datetime.datetime.strptime(str(now), '%Y-%m-%d %H:%M:%S.%f').date()
                diff = relativedelta(ends, start)
                if diff.years < 1:
                    below_1 += 1
                elif diff.years == 1:
                    below_2 += 1
                else:
                    more_than += 1
            except:
                pass

            root_page = urlparse(links_title[i][1]).path
            if root_page and root_page != '/':
                page_count += 1
            else:
                root_count += 1

            check_social_share = pd.Series(social_share)
            check_q = pd.Series(qa_site)
            if ext.domain in check_social_share.values:
                social_count += 1
            elif ext.domain in check_q.values or ext.subdomain in check_q:
                q_a_count += 1
            else:
                aff_match = [True for match in affiliate if match in links_title[i][0].lower()]
                f_match = [True for match in forum if match in links_title[i][1]]
                if True in f_match or any(regex.match(links_title[i][1]) for regex in regexes_forum):
                    forum_count += 1
                    subdomain_remove_www = ext.subdomain.replace("www", "")
                    if subdomain_remove_www:
                        subdomain_remove_www = subdomain_remove_www.replace(".", "")
                        subdomain = subdomain_remove_www + "."
                    else:
                        subdomain = ""
                    forum_url = subdomain + ext.domain + "." + ext.suffix
                    forum_links.append(forum_url)
                elif True in aff_match:
                    affiliate_count += 1
                else:
                    other_count += 1
            if i == len(links_title) - 1:
                max_r = 11 - len(links_title)
                for mr in range(0, max_r):
                    value_empty = ""
                    url = ""
                    registration_date.extend([url, value_empty])
                total_count = [keyword, social_count, q_a_count, forum_count, affiliate_count, other_count,
                               snipper_string, root_count, page_count, below_1, below_2, more_than]
                total_count.extend(registration_date)
                if registration_date_latest:
                    latest_date = max(registration_date_latest).strftime("%m/%d/%Y")
                    # oldest_date = min(registration_date_latest).strftime("%m/%d/%Y")
                else:
                    latest_date = ""
                    # oldest_date = ""
                total_count.extend([latest_date])
                # self.analysis_data.append(total_count)
                final_url_links = list(dict.fromkeys(forum_links))
                for url_link in final_url_links:
                    myquery_link = {"Forum Url": url_link}
                    mydoc_link = mycol_2.find_one(myquery_link)
                    if mydoc_link is None:
                        # print(url_link)
                        columns_fourms = ["Forum Url"]
                        forum_final_url_links = [url_link]
                        # Create a zip object from two lists
                        forumbObj = zip(columns_fourms, forum_final_url_links)

                        # Create a dictionary from zip object
                        forumOfWords = dict(forumbObj)
                        # print(registration_date)
                        x = mycol_2.insert_one(forumOfWords)
                columns = ["Search Keywords", "Social Sharing Site", "Q/A Site", "Forum",
                           "Affiliate Site", "Other", "Snippet", "Root", "Page",
                           "Below 1", "Below 2", "More than", "Site1 Root", "Site1",
                           "Site2 Root", "Site2", "Site3 Root", "Site3", "Site4 Root", "Site4", "Site5 Root", "Site5",
                           "Site6 Root", "Site6", "Site7 Root", "Site7", "Site8 Root", "Site8", "Site9 Root", "Site9",
                           "Site10 Root", "Site10", "Site11 Root", "Site11", "Latest Registration Date"]
                # Create a zip object from two lists
                zipbObj = zip(columns, total_count)

                # Create a dictionary from zip object
                dictOfWords = dict(zipbObj)
                #print(registration_date)
                x = mycol.insert_one(dictOfWords)

    def scrape_info(self, html, s_key):
        soup_all = BeautifulSoup(html, 'html.parser')
        # extract result
        # phrase_extract = soup.find(id="resultStats")
        if soup_all.find('nobr') is not None:
            stats = soup_all.find('nobr').previous_sibling
            to_number = re.sub(r'[^\d+]', '', stats)
        else:
            to_number = 0

        columns_fourms = ["Search Keyword", "All In title"]
        total_result = [s_key, to_number]
        allbObj = zip(columns_fourms, total_result)
        allOfWords = dict(allbObj)
        x = mycol_4.insert_one(allOfWords)

    def post_scrape_callback(self, res, key):
        #print(result.status_code)
        try:
            print(f"For analysis: {res.status_code}")
        except Exception:
            pass
        if res and res.status_code == 200:
            self.scraped_pages.add(key)
            self.parse_links(res.text, key)
            # self.scrape_info(result.text)
        else:
            self.to_crawl.put(key)
            return

    def post_scrape_callback_allintitle(self, res_allintitle, keyword_allintitle):
        try:
            print(f"For all in title: {res_allintitle.status_code}")
        except Exception:
            pass
        if res_allintitle and res_allintitle.status_code == 200:
            self.scraped_pages_allintitle.add(keyword_allintitle)
            self.scrape_info(res_allintitle.text, keyword_allintitle)
        else:
            self.to_crawl_allintitle.put(keyword_allintitle)
            return

    def scrape_page(self, keyword_text):
        try:
            target_url = "https://www.google.com/search?gl=us&hl=en&pws=0&source=hp&q=" + keyword_text + \
                         "&gws_rd=ssl"
            if keyword_text not in self.scraped_pages:
                payload = {'api_key': '', 'url': target_url, 'country_code': 'us'}
                res = requests.get('http://api.scraperapi.com', params=payload)
                self.post_scrape_callback(res, keyword_text)
        except requests.RequestException:
            return

    def scrape_page_allintitle(self, keyword_text):
        try:
            target_url_allintitle = "https://www.google.com/search?gl=us&hl=en&pws=0&source=hp&q=allintitle" + \
                                    keyword_text + "&gws_rd=ssl"
            if keyword_text not in self.scraped_pages_allintitle:
                payload = {'api_key': '', 'url': target_url_allintitle,
                           'country_code': 'us'}
                res_allintitle = requests.get('http://api.scraperapi.com', params=payload)
                self.post_scrape_callback_allintitle(res_allintitle, keyword_text)
        except requests.RequestException:
            return

    def run_scraper(self):
        with ThreadPoolExecutor(max_workers=5) as pool:
            # print(self.to_crawl.qsize())
            iterator = iter(self.to_crawl.get, 'END')
            for chunk in iter(lambda: list(ist.islice(iterator, self.to_crawl.qsize())), []):
                pool.map(self.scrape_page, chunk)
            iterator_allintitle = iter(self.to_crawl_allintitle.get, 'END')
            for chunk in iter(lambda: list(ist.islice(iterator_allintitle, self.to_crawl_allintitle.qsize())), []):
                pool.map(self.scrape_page_allintitle, chunk)
        print(f"Analysis: {self.to_crawl.qsize()} and All in title {self.to_crawl_allintitle.qsize()}")
        # self.export()

    def export(self):
        analysis_data_ap = []
        analysis = mycol.find()
        for item_data in analysis:
            #print(item_data)
            keyword_name = item_data['Search Keywords']
            social_count = item_data['Social Sharing Site']
            q_a_count = item_data['Q/A Site']
            forum_count = item_data['Forum']
            affiliate_count = item_data['Affiliate Site']
            other_count = item_data['Other']
            snipper_string = item_data['Snippet']
            root_count = item_data['Root']
            page_count = item_data['Page']
            below_1 = item_data['Below 1']
            below_2 = item_data['Below 2']
            more_than = item_data['More than']
            if item_data['Site1'] != "":
                site1 = datetime.datetime.strptime(item_data['Site1'], '%m/%d/%Y').date()
            else:
                site1 = ""
            site_root1 = item_data['Site1 Root']
            if item_data['Site2'] != "":
                site2 = datetime.datetime.strptime(item_data['Site2'], '%m/%d/%Y').date()
            else:
                site2 = ""
            site_root2 = item_data['Site2 Root']
            if item_data['Site3'] != "":
                site3 = datetime.datetime.strptime(item_data['Site3'], '%m/%d/%Y').date()
            else:
                site3 = ""

            site_root3 = item_data['Site3 Root']
            if item_data['Site4'] != "":
                site4 = datetime.datetime.strptime(item_data['Site4'], '%m/%d/%Y').date()
            else:
                site4 = ""

            site_root4 = item_data['Site4 Root']
            if item_data['Site5'] != "":
                site5 = datetime.datetime.strptime(item_data['Site5'], '%m/%d/%Y').date()
            else:
                site5 = ""
            site_root5 = item_data['Site5 Root']
            if item_data['Site6'] != "":
                site6 = datetime.datetime.strptime(item_data['Site6'], '%m/%d/%Y').date()
            else:
                site6 = ""
            site_root6 = item_data['Site6 Root']
            if item_data['Site7'] != "":
                site7 = datetime.datetime.strptime(item_data['Site7'], '%m/%d/%Y').date()
            else:
                site7 = ""
            site_root7 = item_data['Site7 Root']
            if item_data['Site8'] != "":
                site8 = datetime.datetime.strptime(item_data['Site8'], '%m/%d/%Y').date()
            else:
                site8 = ""
            site_root8 = item_data['Site8 Root']
            if item_data['Site9'] != "":
                site9 = datetime.datetime.strptime(item_data['Site9'], '%m/%d/%Y').date()
            else:
                site9 = ""
            site_root9 = item_data['Site9 Root']
            if item_data['Site10'] != "":
                site10 = datetime.datetime.strptime(item_data['Site10'], '%m/%d/%Y').date()
            else:
                site10 = ""
            site_root10 = item_data['Site10 Root']
            if item_data['Site11'] != "":
                site11 = datetime.datetime.strptime(item_data['Site11'], '%m/%d/%Y').date()
            else:
                site11 = ""
            site_root11 = item_data['Site11 Root']
            if item_data['Latest Registration Date'] != "":
                latest_date = datetime.datetime.strptime(item_data['Latest Registration Date'], '%m/%d/%Y').date()
            else:
                latest_date = ""
            if all_intitle_switch:
                myquery_final = {"Search Keyword": keyword_name}
                mydoc_final = mycol_4.find_one(myquery_final)
                if mydoc_final:
                    all_in_title = mydoc_final["All In title"]
                else:
                    all_in_title = ""
            else:
                all_in_title = "N/A"
            total_count = [keyword_name, all_in_title, social_count, q_a_count, forum_count, affiliate_count, other_count,
                           snipper_string, root_count, page_count, below_1, below_2, more_than, site_root1, site1,
                           site_root2, site2, site_root3, site3, site_root4, site4, site_root5, site5, site_root6,
                           site6,
                           site_root7, site7, site_root8, site8, site_root9, site9,  site_root10, site10, site_root11,
                           site11, latest_date]
            analysis_data_ap.append(total_count)
            df2 = pd.DataFrame(analysis_data_ap, columns=["Search Keywords", "All In Title", "Social Sharing Site",
                                                            "Q/A Site", "Forum", "Affiliate Site", "Other", "Snippet",
                                                            "Root", "Page", "Below 1", "Below 2", "More than",
                                                            "Site1  Root", "Site1", "Site2  Root", "Site2", "Site3 Root",
                                                            "Site3", "Site4 Root", "Site4", "Site5 Root", "Site5",
                                                            "Site6 Root", "Site6", "Site7 Root", "Site7", "Site8 Root",
                                                            "Site8", "Site9 Root", "Site9", "Site10 Root", "Site10",
                                                            "Site11 Root", "Site11", "Latest Registration Date"])
            forum_final_links =[]
            analysis_forum = mycol_2.find()
            for forum_url in analysis_forum:
                forum_final = [forum_url['Forum Url']]
                forum_final_links.append(forum_final)

            df3 = pd.DataFrame(forum_final_links, columns=["Forum Url"])
            search_suggestion_result_final = []
            analysis_search = mycol_3.find()
            for search_text in analysis_search:
                search_final = [search_text['Search Suggestion Result']]
                search_suggestion_result_final.append(search_final)
            df4 = pd.DataFrame(search_suggestion_result_final, columns=["Keyword"])
            writer = pd.ExcelWriter('analysis_report.xlsx', engine='xlsxwriter')
            df2.to_excel(writer, sheet_name='Sheet1')
            df3.to_excel(writer, sheet_name='Sheet2')
            df4.to_excel(writer, sheet_name='Sheet3')
            worksheet = writer.sheets['Sheet1']
            worksheet.set_column(1, 1, 35)
            worksheet.set_column(2, 6, 20)
            worksheet.set_column(7, 37, 35)
            worksheet2 = writer.sheets['Sheet2']
            worksheet2.set_column(1, 1, 50)
            worksheet3 = writer.sheets['Sheet3']
            worksheet3.set_column(1, 1, 60)
            writer.save()


if __name__ == '__main__':
    s = MultiThreadScraper("http://www.google.com")
    df_input = pd.read_excel(filename)
    if "analysis" in collist and "allintitle" in collist:
        for it in df_input.index:
            keyword = df_input['Keyword'][it]
            myquery = {"Search Keywords": keyword}
            mydoc = mycol.find_one(myquery)
            if mydoc is None:
                s.to_crawl.put(keyword)

            myquery_all = {"Search Keyword": keyword}
            mydoc_all = mycol_4.find_one(myquery_all)
            if mydoc_all is None:
                # print(keyword)
                s.to_crawl_allintitle.put(keyword)
    elif "analysis" in collist:
        for it in df_input.index:
            keyword = df_input['Keyword'][it]
            myquery = {"Search Keywords": keyword}
            mydoc = mycol.find_one(myquery)
            if mydoc is None:
                s.to_crawl.put(keyword)

            myquery_all = {"Search Keyword": keyword}
            mydoc_all = mycol_4.find_one(myquery_all)
            if mydoc_all is None:
                # print(keyword)
                s.to_crawl_allintitle.put(keyword)
    elif "allintitle" in collist:
        for it in df_input.index:
            keyword = df_input['Keyword'][it]
            myquery = {"Search Keywords": keyword}
            mydoc = mycol.find_one(myquery)
            if mydoc is None:
                s.to_crawl.put(keyword)

            myquery_all = {"Search Keyword": keyword}
            mydoc_all = mycol_4.find_one(myquery_all)
            if mydoc_all is None:
                # print(keyword)
                s.to_crawl_allintitle.put(keyword)
    else:
        for it in df_input.index:
            keyword = df_input['Keyword'][it]
            s.to_crawl.put(keyword)
            s.to_crawl_allintitle.put(keyword)
    if s.to_crawl.qsize() >= 1 and s.to_crawl_allintitle.qsize() >= 1:
        s.run_scraper()
