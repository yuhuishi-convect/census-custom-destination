from flask import g
import requests
import logging

LOG = logging.getLogger("airtable")

# methods to implement

AIRTABLE_BASE_API_URL = "https://api.airtable.com/v0/"


def get_airtable_url():
    base_id = g.base_id
    table_id = g.table_id
    api_key = g.api_key
    url = AIRTABLE_BASE_API_URL + base_id + "/" + table_id + "?api_key=" + api_key
    return url


# methods to test if connection to airtable API is working
def test_connection():
    """
    Methods to test if connection to airtable API is working
    Sample response
    {
        "jsonrpc": "2.0",
        "id": "d33ded2672b7877ff833c317892d748c",
        "result": {
            "success": true
        }
    }
    """
    url = get_airtable_url()
    LOG.info(f"Testing connection to airtable API at {url}")
    response = requests.get(url)

    if response.status_code != 200:
        LOG.error(
            f"Error connecting to airtable API. Error code: {response.status_code}. Error message: {response.text}"
        )
        return {"success": False}

    return {"success": True}


# list the supported objects to sync for this custom destination
# In this case, this is the table object specified by the user
def list_objects():
    """
    {
        "jsonrpc": "2.0",
        "method": "list_objects",
        "id": "6d2bd06835a565bee3e2250177f1d738",
        "result": {
            "objects": [
            {
                "object_api_name": "restaurant",
                "label": "Restaurants"
            },
            {
                "object_api_name": "venue",
                "label": "Concert Venues"
            },
            {
                "object_api_name": "reservation",
                "label": "Reservation",
                "can_create_fields": "on_write"
            },
            ]
        }
    }
    """
    table_id = g.table_id
    return {"objects": [{"object_api_name": table_id, "label": table_id.title()}]}


"""
{
  "jsonrpc": "2.0",
  "method": "list_fields",
  "id": "172ac10da8e6e29296612223e428bafd",
  "params": {
    "object": {
      "object_api_name": "restaurant",
      "label": "Restaurants"
    },
  }
}
"""


def list_fields(object):
    """
    Return the fields in the airtable table

    {
        "jsonrpc": "2.0",
        "method": "list_fields",
        "id": "172ac10da8e6e29296612223e428bafd",
        "result": {
            "fields": [
            {
                "field_api_name": "name",
                "label": "Name",
                "identifier": true,
                "required": true,
                "createable": true,
                "updateable": true,
                "type": "string",
                "array": false,
            },
            {
                "field_api_name": "outdoor_dining",
                "label": "Outdoor Dining?",
                "identifier": false,
                "required": false,
                "createable": true,
                "updateable": true,
                "type": "boolean",
                "array": false,
            },
            {
                "field_api_name": "zip",
                "label": "ZIP Code",
                "identifier": false,
                "required": false,
                "createable": true,
                "updateable": true,
                "type": "decimal",
                "array": false,
            },
            {
                "field_api_name": "tag_list",
                "label": "Tags",
                "identifier": false,
                "required": true,
                "createable": true,
                "updateable": true,
                "type": "string",
                "array": true,
            }
            ]
        }
    }
    """

    table_id = object.get("object_api_name")
    if not table_id == g.table_id:
        raise Exception(
            f"Object {table_id} is not consistent with the table {g.table_id}"
        )
    # get the first row of the airtable table
    url = get_airtable_url()
    response = requests.get(url)
    response.raise_for_status()

    records = response.json()["records"]
    if len(records) == 0:
        raise Exception(f"No records found in {table_id}")

    first_row = records[0]
    fields = first_row["fields"]
    field_names = fields.keys()

    result_fields = []
    for field in field_names:
        field_value = fields[field]
        field_type = "string"
        if isinstance(field_value, int):
            field_type = "integer"
        elif isinstance(field_value, float):
            field_type = "decimal"
        is_identifier = False
        if field.lower() == "id" or field.lower() == "name":
            is_identifier = True
        result_fields.append(
            {
                "field_api_name": field,
                "label": field.title(),
                "identifier": is_identifier,
                "required": is_identifier,
                "createable": True,
                "updateable": True,
                "type": field_type,
                "array": False,
            }
        )

    return {"fields": result_fields}


"""
{
  "jsonrpc": "2.0",
  "method": "supported_operations",
  "id": "f6a21a592299ee8ff705354b12f30b4d",
  "params": {
    "object": {
      "object_api_name": "restaurant",
      "label": "Restaurants"
    },
  }
}
"""


def supported_operations(object):
    """
    Return the supported operations
    {
        "jsonrpc": "2.0",
        "method": "supported_operations",
        "id": "f6a21a592299ee8ff705354b12f30b4d",
        "result": {
            "operations": ["upsert", "update"],
        }
    }
    """
    # we only support insert operations
    return {"operations": ["insert"]}


def get_sync_speed():
    """
    Return the rate limit of airtable API
    {
        "jsonrpc": "2.0",
        "id": "fca4af985ea367a22bec9cb6fc31676f",
        "result": {
            "maximum_batch_size": 5,
            "maximum_parallel_batches": 8,
            "maximum_records_per_second": 100,
        }
    }
    """

    return {
        "maximum_batch_size": 10,
        "maximum_parallel_batches": 1,
        "maximum_records_per_second": 50,
    }


"""
{
  "jsonrpc": "2.0",
  "method": "sync_batch",
  "id": "5da3749ace39c7d47791dcf3b10f9842",
  "params": {
    "sync_plan": {
      "object": {
        "object_api_name": "restaurant",
        "label": "Restaurants"
      },
      "operation": "upsert",
      "schema": {
        "name": {
          "active_identifier": true,
          "field": {
            "field_api_name": "name",
            "label": "Name",
            "identifier": true,
            "required": true,
            "createable": true,
            "updateable": true,
            "type": "string",
            "array": false,
          }
        },
        "zip": {
          "active_identifier": false,
          "field": {
            "field_api_name": "zip",
            "label": "ZIP Code",
            "identifier": false,
            "required": false,
            "createable": true,
            "updateable": true,
            "type": "decimal",
            "array": false,
          },
        },
        "tag_list": {
          "active_identifier": false,
          "field": {
            "field_api_name": "tag_list",
            "label": "Tags",
            "identifier": false,
            "required": true,
            "createable": true,
            "updateable": true,
            "type": "string",
            "array": true,
          }
        }
      }
    },
    "records": [
      {
        "name": "Ashley's",
        "zip": "48104",
        "tag_list": ["Bar", "Brewpub"]
      },
      {
        "name": "Seva",
        "zip": "48103",
        "tag_list": ["Vegan", "Casual"]
      },
      {
        "name": "Pizza House",
        "zip": "48104",
        "tag_list": ["Pizzeria", "Sports Bar"]
      },
      {
        "name": "Zingerman's Delicatessen",
        "zip": "48104",
        "tag_list": ["Deli", "Specialty"]
      },
      {
        "name": "Gandy Dancer",
        "zip": "48104",
        "tag_list": ["American", "Seafood", "Historic", "Cocktails"]
      }
    ]
  }
}
"""


def sync_batch(records, sync_plan):
    """
    {
        "jsonrpc": "2.0",
        "id": "5da3749ace39c7d47791dcf3b10f9842",
        "result": {
            "record_results": [
            {
                "identifier": "Ashley's",
                "success": true
            },
            {
                "identifier": "Seva",
                "success": true
            },
            {
                "identifier": "Pizza House",
                "success": false,
                "error_message": "API Error, please retry"
            },
            {
                "identifier": "Zingerman's Delicatessen",
                "success": true
            },
            {
                "identifier": "Gandy Dancer",
                "success": false,
                "error_message": "Exceeded tag limit of 3"
            }
            ]
        }
    }
    """
    url = get_airtable_url()
    schema = sync_plan["schema"]
    record_payload = []
    for record in records:
        # sync each record
        # convert to airtble API payload format
        # map record field name to field_api_name
        mapped_record = {
            schema[k]["field"]["field_api_name"]: record[k]
            for k in record
            if k in schema
        }
        record_payload.append({"fields": {**mapped_record}})

    payload = {"records": record_payload}
    print(f"airtable payload: {payload}")
    print(f"records: {records}")
    print(f"sync_plan: {sync_plan}")

    # send the payload to airtable
    response = requests.post(url, json=payload)

    # locate the identifier field
    identifier_field = None
    for field in schema.keys():
        if schema[field]["active_identifier"]:
            identifier_field = field
            break

    if not identifier_field:
        raise Exception("No identifier field found")

    if response.status_code != 200:
        LOG.error(
            f"Error response from airtable. Error: {response.text}. Error code: {response.status_code}"
        )
        # all the records failed to sync
        record_results = list(
            map(
                lambda record: {
                    "identifier": record[identifier_field],
                    "success": False,
                    "error_message": response.text,
                },
                records,
            )
        )
    else:
        # all the records synced successfully
        record_results = list(
            map(
                lambda record: {
                    "identifier": record[identifier_field],
                    "success": True,
                },
                records,
            )
        )

    return {"record_results": record_results}
