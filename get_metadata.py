# structure:
# "https://kh.google.com/rt/🅐/🅑"
#  - 🅐: planet
#        - "earth"
#        - "mars"
#        - ...
#  - 🅑: resource
#        - "PlanetoidMetadata"
#        - "BulkMetadata/pb=!1m2!1s❶!2u❷"
#           - ❶: octant path
#           - ❷: epoch
#        - "NodeData/pb=!1m2!1s❸!2u❹!2e❺(!3u❻)!4b0"
#           - ❸: octant path
#           - ❹: epoch
#           - ❺: texture format
#           - ❻: imagery epoch (sometimes)


# this is the url
# curl $'https://kh.google.com/rt/tm/earth/BulkMetadata/pb=\u00211m2\u00211s\u00212u992' \
#   -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36' \
#   -H 'Referer: https://earth.google.com/'

#%%

import requests
from google.protobuf.message import Message
from google.protobuf.internal.decoder import _DecodeVarint

def safe_decode_varint(data, pos):
    """Safely decode a varint, returning None if we hit the end of the buffer"""
    try:
        return _DecodeVarint(data, pos)
    except IndexError:
        return None, pos

def decode_message(data, pos):
    """Decode a length-delimited message"""
    result = safe_decode_varint(data, pos)
    if not result[0]:
        return None, len(data)
    
    msg_len, new_pos = result
    if new_pos + msg_len > len(data):
        return None, len(data)
        
    msg_buf = data[new_pos:new_pos + msg_len]
    return msg_buf, new_pos + msg_len

def parse_bulk_metadata(data):
    """Parse BulkMetadata message to get default values"""
    defaults = {
        'timestamp': None,
        'imagery_epoch': None
    }
    
    pos = 0
    while pos < len(data) - 1:
        try:
            field_id = data[pos] >> 3
            wire_type = data[pos] & 0x7
            
            result = safe_decode_varint(data, pos + 1)
            if not result[0]:
                break
                
            value, new_pos = result
            
            if field_id == 5:  # default_imagery_epoch
                defaults['imagery_epoch'] = value
            elif field_id == 6:  # timestamp might be here
                defaults['timestamp'] = value
                
            pos = new_pos
            
        except IndexError:
            break
            
    return defaults

def parse_node_metadata(data):
    """Parse a single NodeMetadata message"""
    node = {}
    pos = 0
    
    while pos < len(data) - 1:
        try:
            field_id = data[pos] >> 3
            wire_type = data[pos] & 0x7
            
            result = safe_decode_varint(data, pos + 1)
            if not result[0]:
                break
                
            value, new_pos = result
            
            if field_id == 1:  # path_and_flags
                node['path'] = value
                node['flags'] = value >> 62
            elif field_id == 2:  # epoch
                node['epoch'] = value
            elif field_id == 5:  # bulk_metadata_epoch (timestamp)
                node['timestamp'] = value
            elif field_id == 7:  # imagery_epoch
                node['imagery_epoch'] = value
            
            pos = new_pos
            
        except IndexError:
            break
            
    return node if 'path' in node else None

# Make the BulkMetadata request
url = "https://kh.google.com/rt/tm/earth/BulkMetadata/pb=!1m2!1s2161535051405072!2u992"
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Referer': 'https://earth.google.com/'
}

print("Making request to BulkMetadata...")
response = requests.get(url, headers=headers)
data = response.content
print(f"Received {len(data)} bytes\n")

# Parse all messages in the response
nodes = []
defaults = None
pos = 0

while pos < len(data):
    msg_buf, new_pos = decode_message(data, pos)
    if not msg_buf:
        break
        
    if len(msg_buf) > 1000:
        print(f"Analyzing message of {len(msg_buf)} bytes...")
        
        # First try to get defaults
        if not defaults:
            defaults = parse_bulk_metadata(msg_buf)
            print(f"Found defaults: {defaults}")
        
        # Then parse nodes
        msg_pos = 0
        while msg_pos < len(msg_buf):
            if msg_buf[msg_pos] == 0x0a:  # Field 1 (node_metadata)
                result = safe_decode_varint(msg_buf, msg_pos + 1)
                if not result[0]:
                    break
                    
                msg_len, next_pos = result
                if next_pos + msg_len > len(msg_buf):
                    break
                    
                node_data = msg_buf[next_pos:next_pos + msg_len]
                node = parse_node_metadata(node_data)
                
                if node:
                    nodes.append(node)
                    
                msg_pos = next_pos + msg_len
            else:
                msg_pos += 1
                
    pos = new_pos

# Use default timestamp if found, otherwise use the one from your example
timestamp = defaults.get('timestamp') if defaults and defaults.get('timestamp') else 1025439

# Show the nodes we found
print(f"\nFound {len(nodes)} nodes:")
for node in nodes:
    if 'imagery_epoch' in node:
        timestamp = node.get('timestamp', 1025439)  # Use node's timestamp or fallback
        url = (f"https://kh.google.com/rt/tm/earth/NodeData/pb="
              f"!1m2!1s{node['path']}!2u990!2e1"
              f"!3u{node['imagery_epoch']}!4b0"
              f"!5i{timestamp}")
        print(f"\nPath: {node['path']}")
        print(f"Epoch: {node.get('epoch')}")
        print(f"Imagery epoch: {node['imagery_epoch']}")
        print(f"Timestamp: {timestamp}")
        print(f"Flags: {node['flags']}")
        print(f"URL: {url}")

# Test one of the NodeData URLs
test_url = "https://kh.google.com/rt/tm/earth/NodeData/pb=!1m2!1s383927!2u990!2e1!3u5!4b0!5i1025439"
response = requests.get(test_url, headers=headers)
print(f"\nTesting NodeData request:")
print(f"Status: {response.status_code}")
print(f"Size: {len(response.content)} bytes")

# %%
