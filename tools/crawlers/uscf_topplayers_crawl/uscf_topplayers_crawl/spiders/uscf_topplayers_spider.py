import scrapy
import json


class USCFTopPlayersSpider(scrapy.Spider):
    """
    use the REST API:
    https://ratings-api.uschess.org/api/v1/top-players/{ListID}
    
    call the API and keep data in old format
    """
    name = "uscftopplayersspider"
    
    API_BASE = "https://ratings-api.uschess.org/api/v1/top-players"
    
    LISTS = {
        # Overall lists
        "RegularOverall": {"rating_type": "Regular", "gender": "Overall", "age": ""},
        "QuickOverall": {"rating_type": "Quick", "gender": "Overall", "age": ""},
        "BlitzOverall": {"rating_type": "Blitz", "gender": "Overall", "age": ""},
        
        # Women's lists
        "WomensRegular": {"rating_type": "Regular", "gender": "Women", "age": ""},
        "WomensQuick": {"rating_type": "Quick", "gender": "Women", "age": ""},
        "WomensBlitz": {"rating_type": "Blitz", "gender": "Women", "age": ""},
        
        # Age lists
        "Regular65Plus": {"rating_type": "Regular", "gender": "Overall", "age": "Age 65+"},
        "Regular50Plus": {"rating_type": "Regular", "gender": "Overall", "age": "Age 50+"},
        "RegularUnder21": {"rating_type": "Regular", "gender": "Overall", "age": "Under 21"},
        "Regular18": {"rating_type": "Regular", "gender": "Overall", "age": "Age 18"},
        "Regular17": {"rating_type": "Regular", "gender": "Overall", "age": "Age 17"},
        "Regular16": {"rating_type": "Regular", "gender": "Overall", "age": "Age 16"},
        "Regular15": {"rating_type": "Regular", "gender": "Overall", "age": "Age 15"},
        "Regular14": {"rating_type": "Regular", "gender": "Overall", "age": "Age 14"},
        "Regular13": {"rating_type": "Regular", "gender": "Overall", "age": "Age 13"},
        "Regular12": {"rating_type": "Regular", "gender": "Overall", "age": "Age 12"},
        "Regular11": {"rating_type": "Regular", "gender": "Overall", "age": "Age 11"},
        "Regular10": {"rating_type": "Regular", "gender": "Overall", "age": "Age 10"},
        "Regular9": {"rating_type": "Regular", "gender": "Overall", "age": "Age 9"},
        "Regular8": {"rating_type": "Regular", "gender": "Overall", "age": "Age 8"},
        "Regular7AndUnder": {"rating_type": "Regular", "gender": "Overall", "age": "Age 7 and Under"},
        
        # Girls lists
        "WomensRegular18": {"rating_type": "Regular", "gender": "Girls", "age": "Age 18"},
        "WomensRegular17": {"rating_type": "Regular", "gender": "Girls", "age": "Age 17"},
        "WomensRegular16": {"rating_type": "Regular", "gender": "Girls", "age": "Age 16"},
        "WomensRegular15": {"rating_type": "Regular", "gender": "Girls", "age": "Age 15"},
        "WomensRegular14": {"rating_type": "Regular", "gender": "Girls", "age": "Age 14"},
        "WomensRegular13": {"rating_type": "Regular", "gender": "Girls", "age": "Age 13"},
        "WomensRegular12": {"rating_type": "Regular", "gender": "Girls", "age": "Age 12"},
    }

    def start_requests(self):
        for list_id, meta in self.LISTS.items():
            url = f"{self.API_BASE}/{list_id}"
            yield scrapy.Request(
                url,
                callback=self.parse_list,
                meta={
                    "list_id": list_id,
                    "rating_type": meta["rating_type"],
                    "gender": meta["gender"],
                    "age": meta["age"],
                },
                headers={"Accept": "application/json"},
            )

    def parse_list(self, response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON from {response.url}")
            return

        list_name = data.get("name", "")
        rating_source = data.get("ratingSource", "R")
        report_date = data.get("reportDate", "")  # Format: YYYY-MM-DD
        list_id = response.meta.get("list_id", "")
        
        if report_date:
            parts = report_date.split("-")
            supp_date = parts[0] + parts[1] if len(parts) >= 2 else ""
        else:
            supp_date = ""
        
        rating_type = response.meta.get("rating_type", "Regular")
        gender = response.meta.get("gender", "Overall")
        age = response.meta.get("age", "")
        
        full_list_name = f"{rating_type} {list_name}"
        if gender == "Women":
            full_list_name = f"Women's {rating_type}"
        elif gender == "Girls":
            full_list_name = f"Girls {rating_type}"
        if age:
            full_list_name = f"{full_list_name} - {age}"
        
        is_only_us = 1 if "AnyFed" not in list_id else 0
        
        # Process player
        for player in data.get("topPlayers", []):
            name = f"{player.get('firstName', '')} {player.get('lastName', '')}".strip()
            
            yield {
                "list": full_list_name,
                "rank": str(player.get("ordinal", "")),
                "id": str(player.get("id", "")),
                "name": name,
                "age": age,
                "is_only_us": str(is_only_us),
                "gender": gender,
                "state": player.get("stateRep", ""),
                "rating_type": rating_type,
                "rating": str(player.get("rating", "")),
                "time": supp_date,
            }
