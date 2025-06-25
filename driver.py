import re
from mysql_db import get_grade as sql_get, set_grade as sql_set
from mongo_db import get_grade as mongo_get, set_grade as mongo_set
from hive_db import get_grade as hive_get, set_grade as hive_set

global_timestamp = 1  

def process_testcase(file_path):
    global global_timestamp

    with open(file_path, "r") as file:
        lines = file.readlines()

    for raw_line in lines:
        line = raw_line.strip().replace(" ", "")  
        if not line:
            continue

        # MERGE detection
        merge_match = re.match(r"(SQL|MONGO|HIVE)\.MERGE\((SQL|MONGO|HIVE)\)", line)
        # print(merge_match)
        if merge_match:
            db1, db2 = merge_match.groups()
            merge(db1.lower(), db2.lower())
            continue

        # GET/SET detection
        match = re.match(r"\d+,(SQL|MONGO|HIVE)\.(GET|SET)\((.*)\)", line)
        if not match:
            print(f" Skipping invalid line: {raw_line}")
            continue
 
        system, op_type, args = match.groups()
        formatted_op = f"{global_timestamp}, {op_type}({args})"  

        # Execute the corresponding GET or SET operation
        if system == "SQL":
            if op_type == "GET":
                sid, cid = [x.strip() for x in args.split(",")]
                result = sql_get(sid, cid)
                
                if result:
                    with open("oplog.sql.txt", "a") as f:
                        f.write(formatted_op + "\n")
            elif op_type == "SET":
                sid, rest = args.split(",", 1)
                cid, grade = [x.strip(" )") for x in rest.rsplit(",", 1)]
                sid = sid.strip(" (")
                result = sql_set(sid, cid, grade)
                
                if result:
                    with open("oplog.sql.txt", "a") as f:
                        f.write(formatted_op + "\n")

        elif system == "MONGO":
            if op_type == "GET":
                sid, cid = [x.strip() for x in args.split(",")]
                result = mongo_get(sid, cid)
                
                if result:
                    with open("oplog.mongo.txt", "a") as f:
                        f.write(formatted_op + "\n")
            elif op_type == "SET":
                sid, rest = args.split(",", 1)
                cid, grade = [x.strip(" )") for x in rest.rsplit(",", 1)]
                sid = sid.strip(" (")
                result = mongo_set(sid, cid, grade)
                
                if result:
                    with open("oplog.mongo.txt", "a") as f:
                        f.write(formatted_op + "\n")

        elif system == "HIVE":
            if op_type == "GET":
                sid, cid = [x.strip() for x in args.split(",")]
                result = hive_get(sid, cid)
            
                if result:
                    with open("oplog.hive.txt", "a") as f:
                        f.write(formatted_op + "\n")
            elif op_type == "SET":
                sid, rest = args.split(",", 1)
                cid, grade = [x.strip(" )") for x in rest.rsplit(",", 1)]
                sid = sid.strip(" (")
                result = hive_set(sid, cid, grade)
                
                if result:
                    with open("oplog.hive.txt", "a") as f:
                        f.write(formatted_op + "\n")

        print(f" Executed: {formatted_op}")
        global_timestamp += 1  

def merge_based_on_oplog(oplog1, oplog2, db1):
    global global_timestamp

    latest_set_operations = {}

    def parse_log(path):
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split(", ", 1)
                if len(parts) == 2:
                    try:
                        timestamp = int(parts[0])
                        operation = parts[1]

                        match = re.match(r"SET\(\(\s*(SID\d+)\s*,\s*(CSE\d+)\s*\)\s*,\s*([A-F])\)", operation)
                        if match:
                            sid, cid, grade = match.groups()
                            key = (sid, cid)
                            if key not in latest_set_operations:
                                latest_set_operations[key] = (timestamp, grade)
                            else:
                                old_timestamp, _ = latest_set_operations[key]
                                if timestamp > old_timestamp:
                                    latest_set_operations[key] = (timestamp, grade)

                    except ValueError:
                        continue

    parse_log(oplog1)
    parse_log(oplog2)

    for (sid, cid), (timestamp_old, grade) in latest_set_operations.items():
        if db1 == "sql":
            sql_set(sid, cid, grade)
        elif db1 == "mongo":
            mongo_set(sid, cid, grade)
        elif db1 == "hive":
            hive_set(sid, cid, grade)

        # formatted_op = f"{global_timestamp}, SET(({sid},{cid}),{grade})"
        formatted_op = f"{timestamp_old}, SET(({sid},{cid}),{grade})"
        with open(f"oplog.{db1}.txt", "a") as f:
            f.write(formatted_op + "\n")

        print(f" Added to oplog.{db1}.txt: {formatted_op}")
        global_timestamp += 1

    print(f" MERGE: Successfully merged {oplog2} into {oplog1}, updated {db1.upper()}")

def merge(db1, db2):
    db1_oplog = f"oplog.{db1}.txt"
    db2_oplog = f"oplog.{db2}.txt"
    merge_based_on_oplog(db1_oplog, db2_oplog, db1)

if __name__ == "__main__":
    process_testcase("testcase.in")
