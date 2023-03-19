import requests, json
import sha3

from pyspark.sql.functions import col, udf
from pyspark.sql.types import MapType, StringType

def get_abi_events(address: str):
    """ Get the contract abi with events schema """
    payload = {
        'address': address,
        'module': 'contract',
        'action': 'getabi',
        'apikey': 'BIDBK7B8A66BHVTTUCZQIDNP4UQUR3VH26'
    }
    print(f"Getting ABI for adddress: {address}")
    response = requests.get("https://api.etherscan.io/api", params=payload)
    
    if response.status_code == 200:
        abi_contract_result = json.loads(response.json()["result"])
        events = []
        for result in abi_contract_result:
            if result["type"] == "event":
                events.append(result)
        return json.dumps(events)
    else:
        raise Exception(f"Unable to get ABI contract for {address=}")

import eth_abi
from eth_utils import decode_hex
from typing import List

def get_signature(event_sig: str):
    """ Compute the signature for given event schema """
    event_sig_bytes = event_sig.encode()
    keccak_256 = sha3.keccak_256()
    keccak_256.update(event_sig_bytes)
    return keccak_256.hexdigest()

def decode_log_data(topics: List[str], data: str, abi_contract:str):
    """ Decode log data using the abi_contract """
#     abi_contract = get_abi_events(address = address)
    abi_contract = json.loads(abi_contract)
    event_signature = None
    transfer_event = None
    for event in abi_contract:
        if event["name"].lower() == "transfer":
            name = event["name"]
            inputs = [param["type"] for param in event["inputs"]]
            inputs = ",".join(inputs)
            event_signature = get_signature(f"{name}({inputs})")
            transfer_event = event
    
    if event_signature == topics[0][2:]:
        attr_name = None
        attr_type = None
        address_names = []
        address_types = []
        for i in transfer_event['inputs']:

            if not i['indexed']:
                attr_type = i['type']
                attr_name = i['name']
            else:
                address_names.append(i['name'])
                address_types.append(i['type'])

        transfer_value = eth_abi.decode_single(attr_type, decode_hex(data))
        address_values = [ eth_abi.decode_single(n, decode_hex(v)) for n, v in zip(address_types, topics[1:])]    
        final_event = dict(zip(address_names, address_values))
        final_event[attr_name]=str(transfer_value)
        return final_event
    else: 
        return None
    
def convert_decimal(value: str):
    """ Assumed that all the ERC20 tokens use 18 decimal points to represent values. Convert the value to correct decimal value. """
    if value:
        return value[0: (len(value)-18)] + "." + value[(len(value) - 18): len(value)]
    else:
        None

# Regiter Spark UDFs to be used for data processing
decode_log = udf(decode_log_data, MapType(StringType(), StringType()))
to_decimal = udf(convert_decimal, StringType())