# coingecko-coin-data-py
- simple python script
- can pull 14k records in about 30 mins (rate limit)
- doesn't hit the rate limit (limit is 8.33 requests/sec and this script does 8 requests/second)
- multi-threaded
- backups/partial saves
- minimal dependencies (direct: PyYAML, requests)
- logging

## usage
- made for the pro api
- make a config.yaml with your `cg_api_key`
- install requirements `pip install -r requirements.txt`
- run `python threaded_cgkcoinlist.py`
- profit

## todo (maybe)
- demo-api/free-api support