//
//  DBError.swift
//  JETDataBase
//
//  Created by Jim Thomas on 2/26/19.
//

import Foundation
import SQLCipher


public enum JetDatabaseError: Error {
	case documentsDirectoryUnavailable
	case databaseResourceMissing(String)          	// (databaseName)
	case depolyedDatabaseExists
	case deployDatabaseCopyFailed(String, String) 	// (databaseName, errorMessage)
	case databaseAlreadyOpen(String, String)      	// (databaseName, connectionName)
	case databaseOpenFailed(String, String)       	// (databaseName, sqliteErrorMessage)
	case bindUnsupportedType(Any, Int)            	// (valueToBind, index)
	case updateFailed(String)                     	// (Sql)
	case badSQL(String)                           	// (Sql)
	case sqlite3Error(Error)                     	// (<Error> trhow by a SQLite3 function
	case sqlite3ResultError(String)					// (Message for error returned by a Sqlite3 function)
	case unknown
}

public class DBSqlError {

	fileprivate static let successStatuses: Set = [SQLITE_OK, SQLITE_DONE, SQLITE_ROW]

	public static func verify(_ status: Int32, sql: String?) -> Result<Bool, JetDatabaseError> {
		guard successStatuses.contains(status) == false else { return Result.success(true) }

		let sqlString: String!
		if let sql = sql {
			sqlString = " - SQL: \"" + sql + "\""
		} else {
			sqlString = ""
		}
		let message = String(cString: sqlite3_errmsg(JetDatabase.shared.database))
		let msg = message + sqlString + " \(status): \(textForSQLStatus(status))"
		NSLog(msg)
		return Result.failure( JetDatabaseError.sqlite3ResultError(msg))
	}

	public static func textForSQLStatus(_ status: Int32) -> String {
		switch status {
			case   0: return "SQLITE_OK: Successful result"
			case   1: return "SQLITE_ERROR Generic error"
			case   2: return "SQLITE_INTERNAL Internal logic error in SQLite"
			case   3: return "SQLITE_PERM Access permission denied"
			case   4: return "SQLITE_ABORT Callback routine requested an abort"
			case   5: return "SQLITE_BUSY The database file is locked"
			case   6: return "SQLITE_LOCKED A table in the database is locked"
			case   7: return "SQLITE_NOMEM A malloc() failed"
			case   8: return "SQLITE_READONLY Attempt to write a readonly database"
			case   9: return "SQLITE_INTERRUPT Operation terminated by sqlite3_interrupt()"
			case  10: return "SQLITE_IOERR Some kind of disk I/O error occurred"
			case  11: return "SQLITE_CORRUPT The database disk image is malformed"
			case  12: return "SQLITE_NOTFOUND Unknown opcode in sqlite3_file_control()"
			case  13: return "SQLITE_FULL Insertion failed because database is full"
			case  14: return "SQLITE_CANTOPEN Unable to open the database file"
			case  15: return "SQLITE_PROTOCOL Database lock protocol error"
			case  16: return "SQLITE_EMPTY Internal use only"
			case  17: return "SQLITE_SCHEMA The database schema changed"
			case  18: return "SQLITE_TOOBIG String or BLOB exceeds size limit"
			case  19: return "SQLITE_CONSTRAINT Abort due to constraint violation"
			case  20: return "SQLITE_MISMATCH Data type mismatch"
			case  21: return "SQLITE_MISUSE Library used incorrectly"
			case  22: return "SQLITE_NOLFS Uses OS features not supported on host"
			case  23: return "SQLITE_AUTH Authorization denied"
			case  24: return "SQLITE_FORMAT Not used"
			case  25: return "SQLITE_RANGE 2nd parameter to sqlite3_bind out of range"
			case  26: return "SQLITE_NOTADB File opened that is not a database file"
			case  27: return "SQLITE_NOTICE Notifications from sqlite3_log()"
			case  28: return "SQLITE_WARNING Warnings from sqlite3_log()"
			case 100: return "SQLITE_ROW sqlite3_step() has another row ready"
			case 101: return "SQLITE_DONE sqlite3_step() has finished executing"
			default:  return "Undocumented SQLite error"
		}
	}
}
