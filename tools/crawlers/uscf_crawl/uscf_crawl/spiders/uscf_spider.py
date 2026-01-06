import scrapy
import re


class USCFSpider(scrapy.Spider):
    name = "uscfspider"

    def start_requests(self):
        url = 'http://www.uschess.org/datapage/events-rated.php'

        formdata = {"mode":"Search","states":"CA","month":"01/2020"}

        states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
                  "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                  "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                  "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
                  "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
        #states = ["NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        #          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
        #months = ["01","02","03","04","05","06","07","08","09","10","11","12"]
        months = ["10","11","12"]
        for state in states:
            for year in range(2023, 2024):
                for month in months:
                    yearmon = str(month) + '/' + str(year)
                    formdata["states"] = state
                    formdata["month"] = yearmon
                    yield scrapy.http.FormRequest(url, callback=self.parse_list, formdata=formdata)

    def parse_list(self, response):
        #print(response.body)

        for href in response.css('td a::attr(href)').extract():
            if href.find('XtblMain.php') != -1:
                #skip any xtbl that points to subsections
                if href[-2] == '.' and href.endswith('0') == -1:
                    continue
                yield response.follow(href, callback=self.parse_event)

    def parse_sections(self, event_id, section_name, lines):
        pairings = dict()
        id_map = dict()
        id_name = dict()
        table_id_map = dict()
        #useful to remember this when there's both regular and quick rating
        uscf_id = ''
        state = ''
        for line in lines:
            if line.find('Xtbl') != -1:
                cols = line.split('|')
                if (len(cols) < 2):
                    continue
                m = re.match('.*<a[^>]*>(.*)<\/a>.*', cols[0]) 
                number = m.group(1)
                m = re.match('.*<a href=\"MbrDtlMain.php\?(.*)\">([^<]*)<\/a>.*', cols[1]) 
                uscf_id = m.group(1)
                name = m.group(2)
                id_name[uscf_id] = name
                id_map[number] = uscf_id

            else:
                cols = line.split('|')
                for col in cols:
                    state = cols[0]
                    match_found = 0
                    m = re.match('\s*(\d+)\s*\/\s*([^\s]*):\s*([^\s]+).*?&gt;\s*(\d+).*', col)
                    if m is not None:
                        uscf_id = m.group(1)
                        rating_type = m.group(2)
                        old_rating = m.group(3)
                        new_rating = m.group(4)
                        match_found = 1
                    else:
                        #when there's both regular and quick rating
                        m = re.match('\s*([A-Z]):\s*([^\s]+).*?&gt;\s*(\d+).*', col)
                        if m is not None:
                            rating_type = m.group(1)
                            old_rating = m.group(2)
                            new_rating = m.group(3)
                            match_found = 1
                    
                    if match_found == 1:
                        if old_rating == 'Unrated' or old_rating == 'Unrated-':
                            old_rating = 0
                        yield {
                            "member": uscf_id,
                            "state": state,
                            "event_id": event_id,
                            "section": section_name,
                            "rating_type": rating_type,
                            "old_rating": old_rating,
                            "new_rating": new_rating
                        }


        for line in lines:
            if line.find('Xtbl') != -1:
                cols = line.split('|')
                if (len(cols) < 2):
                    continue
                m = re.match('.*<a href=\"MbrDtlMain.php\?(.*)\">([^<]*)<\/a>.*', cols[1]) 
                uscf_id = m.group(1)
                count = 0
                for col in cols:
                    count = count + 1
                    if (count < 2):
                        continue
                             
                    m = re.match('(.)\s+(\d+)', col)
                    if (m is not None):
                        result = m.group(1) 
                        opponent = m.group(2)
                        opponent_uscf_id = id_map[opponent]
                        #print(uscf_id, ",", count-3, opponent_uscf_id, ",", result)
                        yield {
                            "member": uscf_id,
                            "event_id": event_id,
                            "section": section_name,
                            "round": count-3,
                            "result": result,
                            "opponent_id": opponent_uscf_id,
                        }


                    m=re.match('^\s*(.)\s*$',col)
                    if (m is not None):
                        result = m.group(1)
                        yield {
                            "member": uscf_id,
                            "event_id": event_id,
                            "section": section_name,
                            "round": count-3,
                            "result": result,
                            "opponent_id": 0,
                        }


    def parse_event(self, response):
        response = response.replace(body=response.body.replace(b'&nbsp;', b''))
        #response = response.replace(body=response.body.replace(b'&gt;', b'>'))
        event_path = response.xpath("//tr[td[@width='70' and contains(text(),'Event')]]/td[3]")
        event_name = event_path.xpath(".//b/text()").get()
        event_id = event_path.xpath(".//small/text()").get()
        event_id = event_id.strip(' )(')
        #print(event_name)
        #print(event_id)

        event_location = response.xpath("//tr[td[@bgcolor=\"EEEEEE\" and contains(text(),'Location')]]/td[2]/b/text()").get()
        #print(event_location)

        event_date = response.xpath("//tr[td[@bgcolor=\"EEEEEE\" and contains(text(),'Event Date')]]/td[2]/b/text()").get()
        #print(event_date)
        sponsoring_affiliate = response.xpath("//tr[td[@bgcolor=\"EEEEEE\" and contains(text(),'Sponsoring')]]/td[2]/b/a/text()").get()
        sponsoring_affiliate_id = response.xpath("//tr[td[@bgcolor=\"EEEEEE\" and contains(text(),'Sponsoring')]]/td[2]/small/text()").get()
        sponsoring_affiliate_id = sponsoring_affiliate_id.strip(' )(')
        #print(sponsoring_affiliate)
        organizer = response.xpath("//tr[td[@bgcolor=\"EEEEEE\" and contains(text(),'Organizer')]]/td[2]/small/text()").get()
        chief_td = response.xpath("//tr[td[@bgcolor=\"EEEEEE\" and contains(text(),'Chief TD')]]/td[2]/small/text()").get()
        #print("chief td is ", chief_td)
        chief_assist_td = response.xpath("//tr[td[@bgcolor=\"EEEEEE\" and contains(text(),'ChiefAssist.TD')]]/td[2]/small/text()").get()
        assist_td = response.xpath("//tr[td[@bgcolor=\"EEEEEE\" and contains(text(),'Assist. TD')]]/td[2]/small/text()").getall()
        other_td = response.xpath("//tr[td[@bgcolor=\"EEEEEE\" and contains(text(),'Other TDs')]]/td[2]/small/text()").getall()

        tournament_stats = response.xpath("//tr[td[@bgcolor=\"EEEEEE\" and contains(text(),'Stats')]]/td[2]/b").get()

        section_stats_match_found = 0

        if tournament_stats is not None:
            m = re.search('\s*(\d+) Section.*?(\d+) Players.*', tournament_stats)

            #section_stats_match = re.match('.*(\d+) Rounds.*?(\d+) Players.*Rating Sys:\s*(.*?)\s*Tnmt Type: (.*)<br>Time Control:(.*)', tournament_stats)
            #if section_stats_match is not None:
            #    section_stats_match_found = 1

            if m is not None:
                num_sections = m.group(1)
                num_players = m.group(2)
                yield {
                    "event_id": event_id,
                    "event_name": event_name,
                    "event_date": event_date,
                    "event_location": event_location,
                    "sponsoring_affiliate": sponsoring_affiliate,
                    "sponsoring_affiliate_id": sponsoring_affiliate_id,
                    "num_sections": num_sections,
                    "num_players": num_players,
                    "organizer": organizer,
                    "chief_td": chief_td,
                    "chief_assist_td": chief_assist_td,
                    "assist_td": assist_td,
                    "other_td": other_td,
                }
                        
        for href in response.css('a::attr(href)').extract():
            if href.find('XtblMain.php') != -1:
                if href[-2] == '.' and href[-1] != '0':
                    continue
                elif href[-3] == '.':
                    continue
                else:
                    yield response.follow(href, callback=self.parse_event)

        #print(num_rounds)
        #print(num_players)
        #print(tournament_type)
        #print(time_control)
        #all_sections = response.xpath("//table[@border=1 and @bgcolor=FFFFFF]").getall()
        all_sections = response.xpath("//table[@border=1]").getall()
        #print(all_sections)
        for section in all_sections:
            #print(section)
            m = re.search('<b>Section \d+\s*\-([^<]*)<\/b>', section)
            #not a valid section
            if m is None:
                continue
            section_name = m.group(1).strip()

            m = re.search('.*(\d+) Rounds.*?(\d+) Players.*Rating Sys:\s*(.*?)\s*Tnmt Type: (.*)', section)
            if m is not None:
                num_rounds = m.group(1)
                num_players = m.group(2)
                rating_sys = m.group(3)
                tournament_type = m.group(4)
                time_control = ''
                m = re.search('.*Time Control: ([^<]*)', section)
                if m is not None:
                    time_control = m.group(1)
                yield {
                    "event_id": event_id,
                    "section_name": section_name,
                    "num_rounds": num_rounds,
                    "tournament_type": tournament_type,
                    "rating_sys": rating_sys,
                    "time_control": time_control,
                }


            #print("section is", section_name)
            #all_pairings = section.xpath("//p/td[@colspan=3]/pre").getall()

            lines = section.splitlines()
            #print(lines)
            for item in self.parse_sections(event_id, section_name, lines):
                yield item
        for href in response.css('a::attr(href)').extract():
            if href.find('MbrDtlMain') != -1:
                yield response.follow(href, callback=self.parse_member)


    def parse_member(self, response):
        def handle_unrated(rating):
            if rating.find("Unrated") != -1:
                return 0
            else:
                m = re.match('\s*([\d]+).*', rating)
                rating = m.group(1)
                return rating

        response = response.replace(body=response.body.replace(b'&nbsp;', b''))
        response = response.replace(body=response.body.replace(b'<br>', b'\n'))
        response = response.replace(body=response.body.replace(b'<nobr>', b''))
        response = response.replace(body=response.body.replace(b'</nobr>', b''))
        member_info = response.xpath("//font/b/text()").get()
        m = re.match('\s*([\d]+):\s*(.*)\s*', member_info)
        member_id = m.group(1)
        member_name = m.group(2)

        regular_rating = response.xpath("//tr[td[contains(text(),'Regular Rating')]]/td[2]/b/text()").get()
        regular_rating = handle_unrated(regular_rating)

        quick_rating = response.xpath("normalize-space(//tr[td[contains(text(),'Quick Rating')]]/td[2]/b/text())").get()
        quick_rating = handle_unrated(quick_rating)


        blitz_rating = response.xpath("normalize-space(//tr[td[contains(text(),'Blitz Rating')]]/td[2]/b/text())").get()
        blitz_rating = handle_unrated(blitz_rating)

        online_regular_rating = response.xpath("normalize-space(//tr[td[contains(text(),'Online-Regular Rating')]]/td[2]/b/text())").get()
        online_regular_rating = handle_unrated(online_regular_rating)

        online_quick_rating = response.xpath("normalize-space(//tr[td[contains(text(),'Online-Quick Rating')]]/td[2]/b/text())").get()
        online_quick_rating = handle_unrated(online_quick_rating)
            
        online_blitz_rating = response.xpath("normalize-space(//tr[td[contains(text(),'Online-Blitz Rating')]]/td[2]/b/text())").get()
        online_blitz_rating = handle_unrated(online_blitz_rating)


        chess_titles = response.xpath("normalize-space(//tr[td[contains(text(),'Chess Titles')]]/td[2]/a/b/text())").getall()
        gender = response.xpath("normalize-space(//tr[td[contains(text(),'Gender')]]/td[2]/b/text())").get()
        state = response.xpath("normalize-space(//tr[td[normalize-space()='State']]/td[2]/b/text())").get()
        fide_id = response.xpath("normalize-space(//tr[td[contains(text(),'FIDE ID')]]/td[2]/b/text())").get()
        fide_country = response.xpath("normalize-space(//tr[td[contains(text(),'FIDE Country')]]/td[2]/b/text())").get()
        yield {
            'name': member_name,
            'id': member_id,
            'regular_rating':regular_rating,
            'quick_rating': quick_rating,
            'blitz_rating': blitz_rating,
            'online_regular_rating': online_regular_rating,
            'online_quick_rating': online_quick_rating,
            'online_blitz_rating': online_blitz_rating,
            'chess_titles': chess_titles,
            'state': state,
            'gender': gender,
            'fide_id': fide_id,
            'fide_country': fide_country,
        }

