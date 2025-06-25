import mysql.connector

def get_grade(student_id, course_id):
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Manish@123",
            database="sample"
        )
        cursor = connection.cursor()

        query = """
            SELECT grade FROM student_grades1
            WHERE student_id = %s AND course_id = %s
        """
        cursor.execute(query, (student_id, course_id))
        print("Done mysql get grade,...")
        result = cursor.fetchone()

        if result:
            print(f" Grade for ({student_id}, {course_id}) is: {result[0]}")
            return True  # Successfully retrieved grade
        else:
            print(f"No record found for ({student_id}, {course_id})")
            return False  # No record found

    except mysql.connector.Error as err:
        print(f" MySQL Error: {err}")
        return False  
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def set_grade(student_id, course_id, grade):
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Manish@123",
            database="sample"
        )
        cursor = connection.cursor()

        q1 = """SELECT grade FROM student_grades1 where student_id = %s AND course_id = %s"""
        cursor.execute(q1,(student_id,course_id))
        res = cursor.fetchone()
        if res:
            if res[0] == grade:
                # print("We found")
                return True

        query = """
            UPDATE student_grades1
            SET grade = %s
            WHERE student_id = %s AND course_id = %s
        """


        cursor.execute(query, (grade, student_id, course_id))
        connection.commit()
        print(cursor.rowcount)
        if cursor.rowcount:
            print(f"Updated grade for ({student_id}, {course_id}) to '{grade}'")
            return True  # Successfully updated grade
        else:
            print(f"No matching record found to update.")
            return False  # No record found to update

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        return False  
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


if __name__ == "__main__":
    set_grade('SID9999','CSE020','H')
    set_grade('SID1033','CSE016','C')
    


