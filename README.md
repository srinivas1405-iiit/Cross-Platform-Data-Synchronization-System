# Data Synchronization Across Heterogeneous Systems

A Python-based system for synchronizing student grade data across **MySQL**, **MongoDB**, and **Apache Hive** while maintaining eventual consistency, conflict resolution, and data auditability.

## 🚀 Overview

This project addresses the challenge of maintaining **data consistency** across heterogeneous databases — a common scenario in educational institutions. The system synchronizes data using **timestamp-based merge operations** and **operation logs**, ensuring that updates to student grades are consistent regardless of the underlying database type.

---

## 📌 Features

- ✅ Unified `GET`, `SET`, and `MERGE` operations across MySQL, MongoDB, and Hive.
- ✅ Timestamp-based conflict resolution (`last-writer-wins` strategy).
- ✅ Operation logging for auditability (`oplog.sql.txt`, `oplog.mongo.txt`, `oplog.hive.txt`).
- ✅ Modular database adapters for easy scalability and extension.
- ✅ Batch test case processing via input files.

---

## 🏗️ System Architecture

- **Database Adapters**:
  - `mysqldb.py`: Handles MySQL-specific operations.
  - `mongodb.py`: Handles MongoDB-specific operations.
  - `hivedb.py`: Handles Hive-specific operations.

- **Driver Program**:
  - `driver.py`: Reads test case files and routes operations to appropriate adapters.

- **Operation Logs**:
  - Text files that record timestamped operations for each database.

- **Merge Engine**:
  - Compares and reconciles operation logs to resolve conflicts and ensure consistency.

---

## 🛠️ How It Works

### ✅ GET Operation
Retrieves the current grade of a `(student_id, course_id)` pair.

### ✅ SET Operation
Updates the grade for a given student-course pair and logs the operation with a timestamp.

### ✅ MERGE Operation
Synchronizes one database with another by:
- Comparing `SET` operations from oplogs.
- Resolving conflicts via latest timestamp.
- Updating target DB and its oplog.

---

## 📂 Example

### Sample Test Case (`testcase1.in`)
```text
1, HIVE.SET((SID1033,CSE016),A)
4, SQL.SET((SID1033,CSE016),B)
HIVE.MERGE(SQL)

# Sample Oplogs After Merge

## Oplogs

**`oplog.hive.txt`**:
1, SET((SID1033,CSE016),A)
4, SET((SID1033,CSE016),B) # Added after merge due to higher timestamp

**`oplog.sql.txt`**:
4, SET((SID1033,CSE016),B)


## 🔁 Merge Properties

- **Associative**: Order of merges doesn’t affect the final result.  
- **Commutative**: `A.merge(B) = B.merge(A)`  
- **Idempotent**: Re-merging the same pair causes no additional changes.  
- **Eventually Consistent**: All databases converge after a complete merge cycle.

## 🧪 Technologies Used

- **Python**: Core implementation and scripting  
- **MySQL**: Relational database engine  
- **MongoDB**: NoSQL document store  
- **Apache Hive**: Distributed data warehousing system built on Hadoop

## 📊 Results

The system was tested using multiple timestamped test case files.  
Screenshots of the final outputs and test cases demonstrate that the merge logic resolves conflicts correctly and ensures consistency across all three databases.

## 📌 Challenges Faced

- Implementing transactional safety in Hive  
- Designing a merge mechanism that respects logical write-time, not test case line order  
- Edge case handling during test case validation and oplog reconciliation

## ✅ Conclusion

This project demonstrates the feasibility of a unified synchronization mechanism across heterogeneous databases.  
Through operation logging and merge-based conflict resolution, consistency is achieved while respecting the native strengths of each database paradigm.
