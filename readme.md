# A Census custom destination to integrate with Airtable API

This a sample implementation to Census custom API.

## Usage

Start the server
```sh
python airtable_destination/server.py
```

Send a sync payload 

```sh
cat sync.json |  http ':5000?base_id=appozac8Pr9CMZVz9&table_id=Todo&api_key=<API_KEY>'  -v 
```

## Deployments

We use [Zappa](https://github.com/zappa/Zappa) to deploy the app as an AWS lambda function.

```sh
pip install zappa

# initial deployment
zappa deploy dev

# later
zappa update dev

# to view endpoint
zappa status dev

# to view the log
zappa tail dev
```

## Reference
[Census custom destination](https://docs.getcensus.com/destinations/custom-api#rpc-details)

