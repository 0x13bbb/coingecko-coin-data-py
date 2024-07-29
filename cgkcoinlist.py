import requests
import json
import logging
import traceback
from config import loadConfig

logging.basicConfig(filename='cgk.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
config = loadConfig()

def prettyPrint(json_data):
    print(json.dumps(json_data, indent=2))

def makeGetRequest(url, headers):
    try:
        response = requests.get(url,headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_error:
        if http_error.response.status_code == 429:
            logger.info("Hit rate limit, sleeping for 20s")
            t.sleep(20)
            return make_get_request(url, headers)
        else:
            raise

def writeJSON(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=2)

def safe_get(d, *keys):
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key)
        else:
            logger.info(f"No such keys found: {keys}")
            return None
    return d

def process_ids():
    list_url = "https://pro-api.coingecko.com/api/v3/coins/list"

    headers = {
        "accept": "application/json",
        "x-cg-pro-api-key": config['cg_api_key']
    }
    list_json = makeGetRequest(list_url, headers)

    info_url = "https://pro-api.coingecko.com/api/v3/coins/{0}"
    raw_output = []
    output = []

    write_interval = 400

    for i in list_json:
        try:
            logger.info(f"Processing {i['id']}")
            info_json = makeGetRequest(info_url.format(i["id"]), headers)
            raw_output.append(info_json)

            normalized_json = {
                "id": safe_get(info_json,"id"),
                "symbol": safe_get(info_json,"symbol"),
                "name": safe_get(info_json,"name"),
                "image": safe_get(info_json,"image", "large"),
                "current_price": safe_get(info_json,"market_data","current_price","usd"),
                "market_cap": safe_get(info_json,"market_data","market_cap","usd"),
                "market_cap_rank": safe_get(info_json,"market_cap_rank"),
                "fully_diluted_valuation": safe_get(info_json,"market_data","fully_diluted_valuation","usd"),
                "total_volume": safe_get(info_json,"market_data","total_volume","usd"),
                "high_24h": safe_get(info_json,"market_data","high_24h","usd"),
                "low_24h": safe_get(info_json,"market_data","low_24h","usd"),
                "price_change_24h": safe_get(info_json,"market_data","price_change_24h"),
                "price_change_percentage_24h": safe_get(info_json,"market_data","price_change_percentage_24h"),
                "market_cap_change_24h": safe_get(info_json,"market_data","market_cap_change_24h"),
                "market_cap_change_percentage_24h": safe_get(info_json,"market_data","market_cap_change_percentage_24h"),
                "circulating_supply": safe_get(info_json,"market_data","circulating_supply"),
                "total_supply": safe_get(info_json,"market_data","total_supply"),
                "max_supply": safe_get(info_json,"market_data","max_supply"),
                "ath": safe_get(info_json,"market_data","ath","usd"),
                "ath_change_percentage": safe_get(info_json,"market_data","ath_change_percentage","usd"),
                "ath_date": safe_get(info_json,"market_data","ath_date","usd"),
                "atl": safe_get(info_json,"market_data","atl","usd"),
                "atl_change_percentage": safe_get(info_json,"market_data","atl_change_percentage","usd"),
                "atl_date": safe_get(info_json,"market_data","atl_date","usd"),
                "roi": safe_get(info_json,"market_data","roi"),
                "last_updated": safe_get(info_json,"last_updated"),
            }
            output.append(normalized_json)
            
            if write_interval == 0:
                logger.info(f"Interval saving progress to disk")
                writeJSON(output, 'new_cgkhardlist.json')
                writeJSON(raw_output, 'raw_cgoutput.json')
                write_interval = 400
                break
            else:
                write_interval -= 1
        except Exception as e:
            writeJSON(output, 'new_cgkhardlist.json')
            writeJSON(raw_output, 'raw_cgoutput.json')
            logger.error(f"Error processing {i['id']}: {e}")
            logger.error(traceback.format_exc())

    logger.info(f"Finished processing, final write")
    writeJSON(output, 'new_cgkhardlist.json')
    writeJSON(raw_output, 'raw_cgoutput.json')

process_ids()