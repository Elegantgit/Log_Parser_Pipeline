import logging
import ijson
import json


file_path = "./crown_interactive_january_logs.json"

# def read_log(file_path, batch_size=1000):
#     batch = []

#     with open(file_path, "r") as f:
#         for record in ijson.items(f, "data.item"):
#             batch.append(record)

#             if len(batch) >= batch_size:
#                 yield batch
#                 batch = []

#     if batch:
#         yield batch


import json

# def read_log(file_path):
#     with open(file_path, "r") as f:
#         for line in f:
#             line = line.strip()
#             if line:
#                 yield json.loads(line)



def read_log(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)   # loads full array
        for record in data:
            yield record
      