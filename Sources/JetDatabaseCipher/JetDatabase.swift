//
//  JETDataBase
//  JETDataBase
//
//  Created by Jim Thomas on 2/26/19.
//

import Foundation
import SQLCipher


public typealias sqlite3_ptr = OpaquePointer
public typealias SqlStringFunction = (([Any?]) -> String?)
public typealias SimpleSQLStringFunction = SqlStringFunction
public typealias SqlInt64Function = (([Any?]) -> Int64?)
public typealias SimpleSQLInt64Function = SqlInt64Function
public typealias JetDatabaseResultRow = [Any?]

typealias sqlite3_stmt_ptr   = OpaquePointer
typealias sqlite3_data_type  = OpaquePointer?

fileprivate typealias SqlFunctionWrapper = @convention(block) (OpaquePointer?, Int32, UnsafeMutablePointer<OpaquePointer?>?) -> Void

internal let ABP_DB_NIL_PTR		= OpaquePointer(bitPattern: 0x0)
internal let SQLITE_STATIC		= unsafeBitCast(0, to: sqlite3_destructor_type.self)
internal let SQLITE_TRANSIENT	= unsafeBitCast(-1, to: sqlite3_destructor_type.self)

public enum RowChangeType {
    case noop
    case insert
    case delete
    case update

    static func type(for sqlOperation: Int32) -> RowChangeType {
        switch sqlOperation {
            case SQLITE_INSERT: return .insert
            case SQLITE_DELETE: return .delete
            case SQLITE_UPDATE: return .update
            default: return .noop
        }
    }
}

// MARK: - Database update/change callback

fileprivate var onDatabaseRowChangeCallback: ((_ tableName: String, _ rowId: Int64, _ changeType: RowChangeType) -> Void)? = nil
fileprivate func onDatabaseRowChange(unused: UnsafeMutableRawPointer?, operation: Int32, tableName: String, rowId: Int64) {
    onDatabaseRowChangeCallback?(tableName, rowId, RowChangeType.type(for: operation))
}

// MARK: - JetDatabase class

public class JetDatabase {

	public static var shared: JetDatabase = JetDatabase()

	// Compiled statement cache
	static var stmtList = [String: sqlite3_stmt_ptr]()

	public var logging: Bool = true
	public var dbPath: String? = nil
	public var database: sqlite3_ptr? = nil
    public var directoryURL: URL?

    fileprivate var dbTransactionId: Int = 0
    fileprivate var dbName: String? = nil
	fileprivate var preserveScopeOfSQLFunctions = [SqlFunctionWrapper]()

    fileprivate var changeHandler: ((_ rowId: Int64, _ changeType: RowChangeType, _ tableName: String) -> Void)? = nil
    fileprivate typealias DBRowChangeCallback = @convention(block) (OpaquePointer, Int32, UnsafeMutablePointer<OpaquePointer?>?, UnsafeMutablePointer<OpaquePointer?>?, Int64) -> Void

	// MARK: - Class Functions

	static func getUnsharedDB() -> JetDatabase {
		return JetDatabase(shared: false)
	}

	// MARK: - Life Cycle

	private	init() {
	}

	private init(shared: Bool) {
		guard shared == false else { return }
	}

	//-------------------------------------------------------------------------
	/// Copies the database from the bundle to the App's documents directory, unless it's aready there.
	/// - Parameter dbName: The name of the database resource. This will also be the name in the documents directory.
	/// - Parameter Bundle: The bundle containing the database to be deployed.
    public func deployDatabase(dbName: String, toDirectory: URL, bundle: Bundle) throws {
		self.dbName = dbName
		guard let resourceUrl: URL = bundle.url(forResource: dbName, withExtension:nil) else { throw(JetDatabaseError.databaseResourceMissing(dbName)) }

		let dbUrl: URL = toDirectory.appendingPathComponent(dbName)
		self.dbPath = dbUrl.path
		let fm: FileManager = FileManager.default
		if fm.fileExists(atPath: dbUrl.absoluteString) {
			throw(JetDatabaseError.depolyedDatabaseExists)
		}
		else {
			do {
				try fm.copyItem(at: resourceUrl, to: dbUrl)
			} catch {
				throw(JetDatabaseError.deployDatabaseCopyFailed(dbName, error.localizedDescription))
			}
		}
	}

	//-------------------------------------------------------------------------
	/// Open a database connection
	/// - Parameter databaseName:Name of the database file
	/// - Parameter connectionName:Name of the database connection. Used to
	/// 						uniquely identify the connection. You can access
	///							the connection
	/// - Returns: true on success.
    public func openDatabase(_ databaseName: String, inDirectory: URL, key: String? = nil) throws {
        try open(dbName: databaseName, inDirectory: inDirectory, key: key)
	}

	//-------------------------------------------------------------------------
	/// Open a database connection
	/// - Parameter databaseName:Name of the database file
	/// - Parameter in: Name of the directory containing the database
    public func open(dbName: String, inDirectory: URL, key: String? = nil) throws {
        directoryURL = inDirectory
		let dbPath: String = inDirectory.appendingPathComponent(dbName).path
		if FileManager.default.fileExists(atPath: dbPath) == false {
			try FileManager.default.createDirectory(atPath: inDirectory.path, withIntermediateDirectories: true, attributes: nil)
		}

		self.dbPath = dbPath
		guard sqlite3_open_v2(dbPath, &database, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, nil) == SQLITE_OK else {
			database = nil;
			let errMessage = String(cString: sqlite3_errmsg(database))
			throw(JetDatabaseError.databaseOpenFailed(dbName, errMessage))
		}

		// Requires SQLCipher
		if let key = key {
			sqlite3_key(database, key, Int32(key.utf8CString.count))
		}
	}

	//-------------------------------------------------------------------------
	/// Reuse a database connection that has already been opened. Maybe you're
	/// using a third party library that opened the database. You can reuse its
	/// handle by passing it to this function.
	/// - parameter dbHandle: A database handle that was opened by some other SQLite3 library.
	public func useOpenedDatabase(dbHandle: Any?) {
		guard  let db = dbHandle as? sqlite3_ptr else { fatalError() }
		database = db
	}

	//-------------------------------------------------------------------------
	/// Close the database connection.
	public func close() {
		self.closeDatabase()
	}

	fileprivate func closeDatabase() {
		if (database != nil) {
			JetDatabaseStatementCache.sharedInstance.finalizeAll()
			sqlite3_close_v2(database)
			database = nil;
		}
	}

    /// The callback closure must not call any SQLite functions that would modify the database connection.
    ///  Calling this function replaces any handlers that were previously setup.
    ///  Note the the rowId is not the primary key setup in the table's schema definition. rowId is specific to SQLite,  is unique for the record, and can change.
    public func rowChangeHandler(userData: Any?, handler: @escaping (_ tableName: String, _ rowId: Int64, _ changeType: RowChangeType) -> Void) {
        onDatabaseRowChangeCallback = handler
        var userData = userData
        sqlite3_update_hook(
            database,
            { (userInfo: UnsafeMutableRawPointer?, operationType: Int32, dbPtr: UnsafePointer<CChar>?, tableNamePtr: UnsafePointer<CChar>?, rowId: sqlite3_int64) in
                let tableName = String(cString: UnsafePointer<CChar>(tableNamePtr!))

                // Can't use self or any context in C-callback, so we call this global function.
                onDatabaseRowChange(unused: userInfo, operation: operationType, tableName: tableName, rowId: rowId)
            },
            &userData
        )
    }

    public func transactionBegin() -> Int {
        if sqlite3_get_autocommit(database) != 0 {
            sqlite3_exec(database, "BEGIN TRANSACTION", nil, nil, nil)
            return 0
        }
        dbTransactionId += 1
        return dbTransactionId
    }

    public func transactionAbort(transactionId: Int, error: Error) throws  {
        if transactionId == 0 {
            sqlite3_exec(database, "ROLLBACK", nil, nil, nil)
            NSLog("Rolling back transaction")
        }
        throw error
    }

    public func TransactionEnd(transactionId: Int) {
        if transactionId == 0 {
            sqlite3_exec(database, "END TRANSACTION", nil, nil, nil)
        }
    }

	//-------------------------------------------------------------------------
	/// Runs the block of code in an SQLite3 transaction. Transactions can be nested.
	/// - Parameter block: The closure to be executed within the SQL transaction.
	public func transaction(_ block: @escaping () throws -> Void) throws {
		var isMasterTransaction = false
		if sqlite3_get_autocommit(database) != 0 {
			sqlite3_exec(database, "BEGIN TRANSACTION", nil, nil, nil)
			isMasterTransaction = true
		}
		do {
			try block()
		} catch {
			if isMasterTransaction {
				sqlite3_exec(database, "ROLLBACK", nil, nil, nil)
				NSLog("Rolling back transaction")
			}
			throw error
		}
		if isMasterTransaction {
			sqlite3_exec(database, "END TRANSACTION", nil, nil, nil)
		}
	}

	// MARK: - Update

	//-------------------------------------------------------------------------
	/// For CREATE, INSERT, and UPDATE statements
	/// - Parameter sql:	The SQL string
	/// - Parameter params:	The values of the fields that are to be bound to the
	///						question mark characters (?) in the SQL string.
	public func execute(_ sql: String, params: [Any?]? = nil, cacheSQL: Bool = true) throws {
		let stmt = try setupStatement(sql, params: params, cacheSQL: cacheSQL)
		let rc = sqlite3_step(stmt)
		defer {
			if !cacheSQL {
				sqlite3_finalize(stmt)
			}
		}

		if rc != SQLITE_DONE {
			NSLog("\(#file):\(#line) JetDatabase Execute statement failed: SQL return code: \(rc) \(DBSqlError.textForSQLStatus(rc))\n" + sql)
			throw(JetDatabaseError.updateFailed(sql))
		}
	}

	// MARK: - Query

	//-------------------------------------------------------------------------
	/// For SELECT statements.
	/// - Parameter sql		The SQL string
	/// - Parameter params	The values of the fields that are to be bound to the
	///						question mark characters (?) in the SQL string.
	///	- Parameter cacheSQL Indicates whether to cache the compiled SQL statement. This avoids
	///						recompiling the SQL in future calls. default = true.
	/// - Returns: List of `JetDatabaseResultRow`s
	public func queryForResultRows(_ sql:String, params: [Any]? = nil, cacheSQL: Bool = true) throws -> [JetDatabaseResultRow] {
        let resultSet: JetDatabaseResultSet = try resultSet(for: sql, params: params, cacheSQL: cacheSQL)
		return resultSet.getResults()
	}

	//-------------------------------------------------------------------------
	/// For SELECT statements.
	/// - Parameter sql		The SQL string
	/// - Parameter params	The values of the fields that are to be bound to the
	///						question mark characters (?) in the SQL string.
	///	- Parameter cacheSQL Indicates whether to cache the compiled SQL statement. This avoids
	///						recompiling the SQL in future calls. default = true.
	/// - Returns: List of dictionaries. Each dictionary contains the key/values for the columns in the row.
	/// 			The column names (dictionary keys) are as specified in the AS clause of the SQL.
	/// 			If there is no AS clause, the column names could change with new releases of SQLite3.
	public func queryForDictionaries(_ sql:String, params: [Any]? = nil, cacheSQL: Bool = true) throws -> [[String: Any?]] {
		let resultSet: JetDatabaseResultSet = try resultSet(for: sql, params: params, cacheSQL: cacheSQL)
		return resultSet.loadDictionaries()
	}

	public func resultSet(for sql: String, params: [Any]? = nil, cacheSQL: Bool = true) throws -> JetDatabaseResultSet {
		let stmt = try setupStatement(sql, params: params, cacheSQL: cacheSQL)
		defer {
			if !cacheSQL {
				sqlite3_finalize(stmt)
			}
		}
		let resultSet = JetDatabaseResultSet(db: self, statement: stmt)
		return resultSet
	}

	fileprivate func setupStatement(_ sql: String, params: [Any?]?, cacheSQL: Bool) throws -> sqlite3_stmt_ptr {
		var stmt: sqlite3_stmt_ptr
		let result = JetDatabaseStatementCache.sharedInstance.compile(sql: sql, db: self, cacheSQL: cacheSQL)
		switch result {
			case .success(let statement): stmt = statement
			case .failure(let error): throw error
		}
		if let params = params {
			try bindStatement(stmt, params: params)
		}
		return stmt
	}

	fileprivate func bindStatement(_ stmt: sqlite3_stmt_ptr, params: [Any?]) throws {
		var colIdx = 1
		do {
			while colIdx <= params.count {
				let valueToBind: Any? = params[colIdx-1]
				try bindObject(valueToBind, toColumn: colIdx, inStatement: stmt)
				colIdx += 1
			}
		} catch {
			NSLog("\(#file):\(#line) Error binding value for param at index \(colIdx)")
			throw error
		}
	}

	fileprivate func bindObject(_ valueToBind: Any?, toColumn index: Int, inStatement stmt: sqlite3_stmt_ptr) throws {
		let idx = Int32(index)

		switch valueToBind {
			case let val as String:
				let cstr = (val as NSString).utf8String
				sqlite3_bind_text(stmt, idx, cstr, -1, nil)

			case let val as Date:
				sqlite3_bind_double(stmt, idx, val.timeIntervalSince1970)

			case let val as Bool:
				sqlite3_bind_int(stmt, idx, (val ? 1 : 0))

			case let val as Int64:
				sqlite3_bind_int64(stmt, idx, val)

			case let val as Int32:
				sqlite3_bind_int64(stmt, idx, Int64(val))

			case let val as Int:
				sqlite3_bind_int64(stmt, idx, Int64(val))

			case let val as Double:
				sqlite3_bind_double(stmt, idx, val)

			case let val as Float:
				sqlite3_bind_double(stmt, idx, Double(val))

			case let val as Data:
				let len = val.count
				let bytes = (val as NSData).bytes
				sqlite3_bind_blob(stmt, idx, bytes, Int32(len), SQLITE_STATIC);

			default:
				sqlite3_bind_null(stmt, idx)
		}
	}

	// MARK: - SQL Function Creation

	public func createStringFunction(functionName: String, argCount: Int32, textType: Int32 = SQLITE_UTF8 | SQLITE_DETERMINISTIC, codeBlock: @escaping SimpleSQLStringFunction) {
		let wrapper: SqlFunctionWrapper = { context, argc, argv in
			let arguments = self.createSQLArguments(argc: argc, argv: argv)
			if let result = codeBlock(arguments) {
				sqlite3_result_text(context, result, Int32(result.count), SQLITE_TRANSIENT)
			}
		}
		createWrappedSQLFunction(wrapper: wrapper, functionName: functionName, argCount: argCount, textType: textType)
	}

	public func createInt64Function(functionName: String, argCount: Int32, textType: Int32 = SQLITE_UTF8 | SQLITE_DETERMINISTIC, codeBlock: @escaping SimpleSQLInt64Function) {
		let wrapper: SqlFunctionWrapper = { context, argc, argv in
			let arguments = self.createSQLArguments(argc: argc, argv: argv)
			if let result = codeBlock(arguments) {
				sqlite3_result_int64(context, result)
			}
		}
		createWrappedSQLFunction(wrapper: wrapper, functionName: functionName, argCount: argCount, textType: textType)
	}

	fileprivate func createSQLArguments(argc: Int32, argv: UnsafeMutablePointer<OpaquePointer?>?) -> [Any?] {
		var arguments: [Any?] = []
		if let argv = argv {
			for idx in 0..<Int(argc) {
				arguments.append(self.dataForSqlValue(argv[idx]))
			}
		}
		return arguments
	}

	fileprivate func createWrappedSQLFunction(wrapper: @escaping SqlFunctionWrapper, functionName: String, argCount: Int32, textType: Int32) {
		let wrappedFn = unsafeBitCast(wrapper, to: UnsafeMutableRawPointer.self)
		sqlite3_create_function_v2(database, functionName, argCount, textType, wrappedFn, { context, argc, value in
			let unWrappedFn = unsafeBitCast(sqlite3_user_data(context), to: SqlFunctionWrapper.self)
			unWrappedFn(context, argc, value)
		}, nil, nil, nil)
		preserveScopeOfSQLFunctions.append(wrapper)
	}

	private func dataForSqlValue(_ value: sqlite3_data_type?) -> Any? {
		if let value = value  {
			switch sqlite3_value_type(value) {
			case SQLITE_BLOB:
				return Data(bytes: sqlite3_value_blob(value), count: Int(sqlite3_value_bytes(value)))
			case SQLITE_FLOAT:
				return sqlite3_value_double(value)
			case SQLITE_INTEGER:
				return sqlite3_value_int64(value)
			case SQLITE_NULL:
				return nil
			case SQLITE_TEXT:
				return String(cString: UnsafePointer(sqlite3_value_text(value)))
			case let type:
				fatalError("\(#file):\(#line) unsupported value type: \(type)")
			}
		}
		return nil
	}
}
