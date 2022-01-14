//
//  JetDatabaseStatementCache.swift
//  JETDataBase
//
//  Created by Jim Thomas on 2/26/19.
//

import Foundation
import SQLCipher


class JetDatabaseStatementCache: NSObject {

	static let sharedInstance = JetDatabaseStatementCache()

	var statements = [String: sqlite3_stmt_ptr]()

	override fileprivate init() {
		super.init()
	}

	func finishStatement(_ stmt: sqlite3_stmt_ptr) {
		var delKey: String? = nil
		for (key,value) in statements {
			if value == stmt {
				delKey = key
				sqlite3_finalize(stmt)
				break
			}
		}
		if delKey != nil {
			statements[delKey!] = nil
		}
	}

	func finalizeAll() {
		for (_,stmt) in statements {
			sqlite3_finalize(stmt)
		}
		statements.removeAll()
	}

	func finishSql(_ sql: String) {
		var delKey: String? = nil
		for (key, stmt) in statements {
			if key == sql {
				delKey = key
				sqlite3_finalize(stmt)
				break
			}
		}
		if delKey != nil {
			statements[delKey!] = nil
		}
	}

	func finishAll() {
		for stmt in statements.values {
			sqlite3_finalize(stmt)
		}
		statements.removeAll()
	}

	//-------------------------------------------------------------------------
	/// This may return ABP_DB_NIL_PTR which is what the value of a nil sqlite3_stmt_ptr.
	func compile(sql: String, db: JetDatabase, cacheSQL: Bool) -> Result<sqlite3_stmt_ptr, JetDatabaseError> {
		// Get cached compiled statement
		let compiledStatement: sqlite3_stmt_ptr? = cacheSQL ? statements[sql] : nil

		// If it wasn't in the cache, compile it and add it to the cache.
		if compiledStatement == nil || compiledStatement == ABP_DB_NIL_PTR {
			return compileStatement(sql, db: db, cacheSQL: cacheSQL)
		} else {
			// We're reusing a previously compiled statement so reset it.
			let status = sqlite3_reset(compiledStatement!)
			if (status != SQLITE_OK) {
				NSLog("\(#file):\(#line) Cannot reuse SQL statement. Maybe it had errors in a previous use of sqlite3_step. Recompiling statement\n" + sql + "\n")
				finishStatement(compiledStatement!)
				return compileStatement(sql, db: db, cacheSQL: cacheSQL)
			} else {
				// Not needed. The parameters get bound before executing the statement.
				//sqlite3_clear_bindings(compiledStatement)
			}
		}
		if let stmt = compiledStatement {
			return Result.success(stmt)
		}
		return Result.failure(JetDatabaseError.badSQL(sql))
	}

	fileprivate func compileStatement(_ statement: String, db: JetDatabase, cacheSQL: Bool) -> Result<sqlite3_stmt_ptr, JetDatabaseError> {
		var compiledStatement: sqlite3_stmt_ptr? = nil
		let sqlResult = sqlite3_prepare_v2(db.database, statement, -1, &compiledStatement, nil)
		let result = DBSqlError.verify(sqlResult, sql: statement)
		switch result {
			case .success(_):
				guard let compiled = compiledStatement else {
					return Result.failure(JetDatabaseError.unknown)
				}
				if cacheSQL {
					statements[statement] = compiled
				}
				return Result.success(compiled)
			case .failure(let error):
				return Result.failure(error)
		}
	}
}
