import logging
import pandas as pd
from ingestion import read_log
from transform import transform_record

logging.basicConfig(level=logging.INFO)

# def run_pipeline(file_path, limit=10):
#     count = 0


#     for record in read_log(file_path):
#         try:
#             transformed = transform_record(record)

#             print(transformed)   # 👈 THIS IS YOUR OUTPUT

#             count += 1
#             if count >= limit:   # 👈 avoid flooding terminal
#                 break

#         except Exception as e:
#             logging.error(f"Error processing record: {e}")




def run_pipeline(file_path, limit=10):
    rows = []   # 👈 store transformed records here
    count = 0
    
    # for record in read_log(file_path):
    #     print("RAW RECORD:", record)
    #     print("TYPE:", type(record))
    #     break
            
    # for record in read_log(file_path):
        # try:
        #     # 👇 if record is a list, loop inside it
        #     if isinstance(record, list):
        #         for item in record:
        #             transformed = transform_record(item)
        #             if transformed:
        #                 rows.append(transformed)
        #     else:
        #         transformed = transform_record(record)
        #         if transformed:
        #             rows.append(transformed)

        # except Exception as e:
        #      logging.error(f"Error processing record: {e}")


    for record in read_log(file_path):
        try:
            transformed = transform_record(record)
            rows.append(transformed)   # 👈 collect instead of print

            count += 1
            if count >= limit:
                break

        except Exception as e:
            logging.error(f"Error processing record: {e}")


    # convert to DataFrame after loop
    df = pd.DataFrame(rows)

    print(df)   # 👈 now you see a table
    return df


if __name__ == "__main__":
    run_pipeline("./Log2/crown_interactive_january_logs.json")
