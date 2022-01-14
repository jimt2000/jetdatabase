//
//  JetDatabaseResultSet.swift
//  JETDataBase
//
//  Created by Jim Thomas on 2/26/19.
//

import Foundation
import SQLCipher


//-------------------------------------------------------------------------
/// Represents the results of an SQL query (SELECT statement)
public class JetDatabaseResultSet {
	fileprivate enum DBResultSetState {
		case none
		case done
		case ready
		case pending
		case error
	}

	fileprivate var state: DBResultSetState
	fileprivate var statement: sqlite3_stmt_ptr
	fileprivate var db: JetDatabase

	internal init(db: JetDatabase, statement: sqlite3_stmt_ptr) {
		self.db = db
		self.statement = statement
		self.state = .pending
	}

	fileprivate func finish() {
		sqlite3_finalize(statement)
	}

	final func getResults() -> [JetDatabaseResultRow] {
		guard state != .none else { fatalError("\(#file):\(#line) Unknown state for DBResultSet.getResults()") }

		var row = [JetDatabaseResultRow]()
		guard state != .done else { return row }

		if state == .pending {
			state = .ready
		}

		var rc = sqlite3_step(statement)
		while rc == SQLITE_ROW {
			let obj = loadRow(statement)
			row.append(obj)
			rc = sqlite3_step(statement)
		}

		if (rc == SQLITE_DONE) {
			state = .done
		} else {
			state = .error
			let msg = String(cString: sqlite3_errmsg(db.database))
            NSLog("\(#file):\(#line) Error: " + msg)
		}
		return row
	}

	fileprivate func loadRow(_ stmt: sqlite3_stmt_ptr) -> JetDatabaseResultRow {
		var columns = JetDatabaseResultRow()

		let columnCount = sqlite3_column_count(stmt)
		for columnIdx: Int32 in 0 ..< columnCount {
			let val: Any? = valueForColumnAtIndex(columnIdx)
			columns.append(val)
		}
		return columns
	}

    fileprivate func loadRowDictionary(_ stmt: sqlite3_stmt_ptr) -> [String: Any?] {
        let columnCount = sqlite3_column_count(statement)
        var columns: [String: Any?] = [:]

        for columnIdx: Int32 in 0 ..< columnCount {
            var columnName: String
            if let columnNameRaw = sqlite3_column_name(statement, columnIdx) {
                let sVal = String(cString: UnsafePointer<Int8>(columnNameRaw))
                columnName = sVal
            } else {
                columnName = "columnIndex\(columnIdx)"
            }

            let val: Any? = valueForColumnAtIndex(columnIdx)
            columns[columnName] = val
        }
        return columns
    }

    
	//-------------------------------------------------------------------------
	/// - Returns: A list of dictionaries. Each dictionary contains the values for each
	/// column of the row.
	/// The column names (dictionary keys) are as specified in the AS clause. If there is no AS clause,
	/// the column names could change with new release sof SQLite3.
	final func loadDictionaries() -> [[String: Any?]] {
        var dictionaries = [[String: Any?]]()
		guard state != .none else {
            NSLog("\(#file):\(#line) Invalid state \(state) for DBResultSet.loadDictionaries()")
            return dictionaries
        }

		if state == .pending {
			state = .ready
		}

		var rc = sqlite3_step(statement)
		while rc == SQLITE_ROW {
            let dictionary = loadRowDictionary(statement)
            dictionaries.append(dictionary)
			rc = sqlite3_step(statement)
		}

		if (rc == SQLITE_DONE) {
			state = .done
		} else {
			state = .error
			let msg = String(cString: sqlite3_errmsg(db.database))
            NSLog("\(#file):\(#line) Error: " + msg)
		}
		return dictionaries
	}

	fileprivate func valueForColumnAtIndex(_ columnIndex: Int32) -> Any? {
		let columnType = sqlite3_column_type(statement, columnIndex)

			var val: Any? = nil
			switch columnType {
				case SQLITE_INTEGER:
					val = sqlite3_column_int64(statement, columnIndex)
				case SQLITE_FLOAT:
					val = sqlite3_column_double(statement, columnIndex)
				case SQLITE_BLOB:
					let len = sqlite3_column_bytes(statement, columnIndex)
					if let raw = sqlite3_column_blob(statement, columnIndex) {
						val = Data(bytes: raw, count: Int(len))
					}
				case SQLITE_TEXT:
					if let valRaw = sqlite3_column_text(statement, columnIndex) {
						val = String(cString: valRaw)
					}
				case SQLITE_NULL:
					val = nil
				default: break
			}
		return val
	}
}

extension JetDatabaseResultSet: Sequence, IteratorProtocol {
    
	// To allow iteration, as in: for row in resultSet {...}
    final public func next() -> [String: Any?]? {
        var dictionary: [String: Any?]?
        performNextRow() { stmt in
            dictionary = loadRowDictionary(stmt)
        }
        return dictionary
    }
    
	final public func nextResultRow() -> JetDatabaseResultRow? {
        var resultRow: JetDatabaseResultRow?
        performNextRow() { stmt in
            resultRow = loadRow(stmt)
        }
        return resultRow
	}
    
    private func performNextRow<T>( rowLoader: ((_ statement: sqlite3_stmt_ptr) -> T?) ) -> T? {
        guard state != .done else { return nil }
        if state == .pending {
            state = .ready
        }

        let rc = sqlite3_step(statement)
        if rc == SQLITE_ROW {
            return rowLoader(statement)
        }
        else if (rc == SQLITE_DONE) {
            state = .done
        }
        else {
            state = .error
            let msg = String(cString: sqlite3_errmsg(db.database)) + " \(rc): \(DBSqlError.textForSQLStatus(rc))"
            NSLog("\(#file):\(#line) \(msg)")
        }
        return nil
    }
}
