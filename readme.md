# A Census custom destination to integrate with Airtable API

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

## Reference
[Census custom destination](https://docs.getcensus.com/destinations/custom-api#rpc-details)

