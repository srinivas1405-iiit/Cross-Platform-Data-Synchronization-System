import pymongo


def get_grade(student_id, course_id):
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["sample"]
        collection = db["student_grades1"]

        result = collection.find_one({"student-ID": student_id, "course-id": course_id})
        if result:
            print(f"Grade for ({student_id}, {course_id}) is: {result['grade']}")
            return True  # Successfully retrieved grade
        else:
            print(f"No record found for ({student_id}, {course_id})")
            return False  

    except pymongo.errors.PyMongoError as err:
        print(f"MongoDB Error: {err}")
        return False  
    finally:
        client.close()


def set_grade(student_id, course_id, grade):
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["sample"]
        collection = db["student_grades1"]

        result = collection.update_one(
            {"student-ID": student_id, "course-id": course_id},
            {"$set": {"grade": grade}}
        )

        if result.matched_count:
            print(f"Updated grade for ({student_id}, {course_id}) to '{grade}'")
            return True  # Successfully updated grade
        else:
            print(f"No matching record found to update.")
            return False  # No matching record found to update

    except pymongo.errors.PyMongoError as err:
        print(f"MongoDB Error: {err}")
        return False  
    finally:
        client.close()

if __name__ == "__main__":
    set_grade('SID9999','CSE020','H')
    set_grade('SID1033','CSE016','C')
    