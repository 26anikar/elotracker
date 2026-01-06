import scrapy
import re

TAG_RE = re.compile(r'<[^>]+>')

def remove_tags(text):
    return TAG_RE.sub('', text)

class FIDESpider(scrapy.Spider):
    name = "fidespider"

    def start_requests(self):
        url = 'https://ratings.fide.com/rated_tournaments.phtml'

        formdata = {"mode":"Search","states":"CA","month":"01/2020"}

        
        countries = ["USA","AFG","ALB","ALG","AND","ANG","ANT","ARG","ARM","ARU","AUS","AUT","AZE","BAH","BRN","BAN","BAR","BLR","BEL","BIZ","BER","BHU","BOL","BIH","BOT","BRA","IVB","BRU","BUL","BUR","BDI","CAM","CMR","CAN","CPV","CAY","CAF","CHA","CHI","CHN","TPE","COL","COM","CRC","CIV","CRO","CUB","CYP","CZE","COD","DEN","DJI","DMA","DOM","ECU","EGY","ESA","ENG","GEQ","ERI","EST","SWZ","ETH","FAI","FIJ","FIN","FRA","GAB","GAM","GEO","GER","GHA","GRE","GRN","GUM","GUA","GCI","GUY","HAI","HON","HKG","HUN","ISL","IND","INA","IRI","IRQ","IRL","IOM","ISR","ITA","JAM","JPN","JCI","JOR","KAZ","KEN","KOS","KUW","KGZ","LAO","LAT","LBN","LES","LBR","LBA","LIE","LTU","LUX","MAC","MAD","MAW","MAS","MDV","MLI","MLT","MTN","MRI","MEX","MDA","MNC","MGL","MNE","MAR","MOZ","MYA","NAM","NRU","NEP","NED","AHO","NZL","NCA","NIG","NGR","MKD","NOR","OMA","PAK","PLW","PLE","PAN","PNG","PAR","PER","PHI","POL","POR","PUR","QAT","ROU","RUS","RWA","SKN","LCA","VIN","SMR","STP","KSA","SCO","SEN","SRB","SEY","SLE","SGP","SVK","SLO","SOL","SOM","RSA","KOR","SSD","ESP","SRI","SUD","SUR","SWE","SUI","SYR","TJK","TAN","THA","TLS","TOG","TGA","TTO","TUN","TUR","TKM","UGA","UKR","UAE","USA","URU","ISV","UZB","VAN","VEN","VIE","WLS","YEM","ZAM","ZIM"]
        #states = ["NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        #          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
        months = ["01","02","03","04","05","06","07","08","09","10","11","12"]
        #months = ["10","11","12"]

        for country in countries:
            for year in range(2021, 2024):
                for month in months:
                    date = str(year) + '-' + month + '-01'
                    url = 'https://ratings.fide.com/a_tournaments.php?'
                    url = url + 'country=' + country
                    url = url + '&period=' + date

                    url = 'https://ratings.fide.com/report.phtml?event=348197'
                    yield scrapy.http.FormRequest(url, callback=self.parse_event_report)
                    #yield scrapy.http.FormRequest(url, callback=self.parse_country_monthlist)

    def parse_country_monthlist(self, response):
        #print(response.body)
        matches = re.findall('(event=\d+)', response.text, re.DOTALL)
        for match in matches:
            url = 'https://ratings.fide.com/report.phtml?' + match
            print("url is ", url)
            yield response.follow(url, callback=self.parse_event_report)



    def parse_tournament_info(self, response):
        event_id = ''
        event_name = ''
        city = ''
        country = ''
        num_players = ''
        hybrid = ''
        category = ''
        start_date = ''
        end_date = ''
        type = ''
        time_control = ''
        for row in response.xpath('//table/tr'):
            cols = row.xpath('./td').getall()
            count = 0
            col_name = ''
            for col in cols:
                col = remove_tags(col)
                col = col.strip()
                if count == 0:
                    col_name = col
                count = count + 1
                if count == 1:
                    if col_name == 'Event code':
                        event_id = col
                    if col_name == 'Tournament Name':
                        event_name = col            
                    if col_name == 'City'
                        city = col
                    if col_name == 'Country':
                        country = col
                    if col_name == 'Category':
                        category = col
                    if col_name == 'Start Date':
                        start_date  = col
            
    def parse_event_report(self, response):
        event_name = response.xpath("//div[@class=\"title-page col-12\"]/text()").get()
        event_name = event_name.strip()
        event_id = ''
        for href in response.css('a::attr(href)').extract():
            if href.find('tournament_information.phtml?event=') != -1:
                event_id = href[35:]
        event_id = event_id.strip()
        
        players = []
        row_count = 0
        for row in response.xpath('//table[@class=\"table2\"]/tr'):
            row_count = row_count + 1
            if row_count == 1:
                continue
            cols = row.xpath('./td').getall()
            count = 0
            player_id = ''
            player_name = ''
            points = ''
            games = ''
            rating_change = ''
            player_info = {}
            for col in cols:
                col = remove_tags(col)
                col = col.strip()
                if count == 0:
                    player_id = col
                    
                if count == 1:
                    player_name = col
                    player_name = ' '.join(reversed(player_name.split(', ')))
                if count == 6:
                    points = col
                if count == 7:
                    games = col
                if count == 8:
                    rating_change = col
                count = count+ 1

            player_info['player_id'] = player_id
            player_info['player_name'] = player_name
            player_info['points'] = points
            player_info['games'] = games
            player_info['rating_change'] = rating_change
            print(player_info)

            players.append(player_info)
                    
        yield {
            "event_id": event_id,
            "event_name": event_name,
            "players": players,
        }
                        
        for href in response.css('a::attr(href)').extract():
            if href.find('tournament_information.phtml') != -1:
                yield response.follow(href, callback=self.parse_tournament_info)

