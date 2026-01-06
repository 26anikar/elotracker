import scrapy
import re


class FIDETopPlayersSpider(scrapy.Spider):
    name = "fidetopplayersspider"

    def start_requests(self):
        start_urls = ['https://ratings.fide.com/a_top.php?list=girls_rapid', 
                      'https://ratings.fide.com/a_top.php?list=men_rapid',
                      'https://ratings.fide.com/a_top.php?list=women_rapid',
                      'https://ratings.fide.com/a_top.php?list=juniors_rapid',
                      'https://ratings.fide.com/a_top.php?list=open', 
                      'https://ratings.fide.com/a_top.php?list=women',
                      'https://ratings.fide.com/a_top.php?list=juniors',
                      'https://ratings.fide.com/a_top.php?list=girls',
                      'https://ratings.fide.com/a_top.php?list=girls_blitz', 
                      'https://ratings.fide.com/a_top.php?list=men_blitz',
                      'https://ratings.fide.com/a_top.php?list=women_blitz',
                      'https://ratings.fide.com/a_top.php?list=juniors_blitz']                  
        for url in start_urls:
            yield scrapy.http.Request(url, callback=self.parse_list)


    def parse_list(self, response):
        list_name = response.xpath("//div[@class=\"title-page col-12\"]/text()").get()
        list_name = list_name.strip()
        #print("list name is ", list_name)
        month = ''
        year = ''
        m = re.search('(.*) ([^ ]*) (\d\d\d\d)$', list_name)
        if m is not None:
            month = m.group(2).strip()
            year = m.group(3).strip()
            list_name = m.group(1).strip()
        supp_date = ''
        if (month == 'December'):
            supp_date = year + '12'
        elif (month == 'November'):
            supp_date = year + '11';
        elif (month == 'October'):
            supp_date = year + '10'
        elif (month == 'September'):
            supp_date = year + '09'
        elif (month == 'August'):
            supp_date = year + '08'
        elif (month == 'July'):
            supp_date = year + '07'
        elif (month == 'June'):
            supp_date = year + '06'
        elif (month == 'May'):
            supp_date = year + '05'
        elif (month == 'April'):
            supp_date = year + '04'
        elif (month == 'March'):
            supp_date = year + '03'
        elif (month == 'February'):
            supp_date = year + '02'
        elif (month == 'January'):
            supp_date = year + '01'
        
        rows = response.xpath('//table/tr').getall()
        #print(rows)
        for row in rows:
            #two cases - one where htere are 7 columsn including age (for age specific lists)
            # another where there are 6 columsn
            row = row.replace('\r', ' ')
            row = row.replace('\n', ' ')
            print(row)
            m = re.search('<tr>\s*<td>(\d+)<\/td>\s*<td>\s*<a href=[^>]*\/(\d+).?>([^<]*)<\/a>\s*<\/td>\s*<td[^>]*>\s*<img[^>]*>\s*([^<]*)\s*<\/td>\s*<td>\s*(\d+)\s*<\/td>', row)
            if m is not None:
                print("match")
                rank = m.group(1).strip()
                name = m.group(3).strip()
                #convert from commas to normal
                comma_pos = name.find(',')
                if comma_pos != -1:
                    name = name[comma_pos+1:] + ' ' +  name[0:comma_pos]
                name = name.strip()
                #print("Got name is ", name)
                id = m.group(2).strip()
                country = m.group(4).strip()
                rating = m.group(5).strip()
                yield {
                        "list": list_name, 
                        "rank": rank,
                        "id": id,
                        "name": name,
                        "age": '',
                        "gender": '',
                        "state": country, 
                        "rating_type": '',
                        "rating": rating,
                        "live_rating": '',
                        "time": supp_date,
                }

