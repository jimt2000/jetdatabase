//
//  JetDatabaseTests.swift
//
//  Created by Jim Thomas on 9/15/15.
//

import XCTest
@testable import JetDatabaseCipher

class JetDatabaseCipherTests: XCTestCase {

    fileprivate struct Person {
        var id: Int64
        var firstName: String?
        var lastName: String?
        var birthdate: Date?
        var photo: Data?
        var bedTime: Date?
        var numberOfFriends: Int64?
        var gpa: Double?
    }
    
    fileprivate struct StoneAgePerson {
        let id: Int = 0
        let first: String
        let last: String
        let score1: Int
        let score2: Int
        let score3: Int
        var params: [Any] { [first, last, score1, score2, score3] }
    }

    fileprivate var fred = Person(id: -1,
                                  firstName: "Fred",
                                  lastName: "Flintstone",
                                  birthdate: Date(timeIntervalSinceNow: (60 * 60 * 24 * 365 * 33 * -1)),    // 33 years ago
                                  photo: String("Pretend\0Photo1\0Data1").data(using: String.Encoding.utf8)!,
                                  bedTime: Date(timeIntervalSince1970: 22 * 30 * 60),    // 10:00pm
                                  numberOfFriends: 132,
                                  gpa: 3.51)
    fileprivate var barney = Person(id: -1,
                                    firstName: "Barney",
                                    lastName: "Rubbel",
                                    birthdate: Date(timeIntervalSinceNow: (60 * 60 * 24 * 365 * 30 * -1)),    // 30 years ago
                                    photo: String("Pretend\0Photo2\0Data2").data(using: String.Encoding.utf8)!,
                                    bedTime: Date(timeIntervalSince1970: 43 * 30 * 60), // 9:00pm (21:30), 43 half hours
                                    numberOfFriends: 482,
                                    gpa: 2.99)

    fileprivate var tempDirectory: URL = {
        let dirList: [String] = NSSearchPathForDirectoriesInDomains(FileManager.SearchPathDirectory.documentDirectory, FileManager.SearchPathDomainMask.userDomainMask, true) as [String]
        let dirPath = dirList[0];
        #if os(macOS)
            return FileManager.default.temporaryDirectory
        #else
            let paths: [URL] = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)
            return paths[0]
        #endif
    }()
    
    fileprivate var dbDirectory: URL {
        return tempDirectory.appendingPathComponent("databases")
    }
    
    override func setUp() {
        super.setUp()

        // Delete any files in the documents directory where these test put files.
        // Files should only exist in the documents directory if a test was
        // aborted prematurely, such as when debugging. Otherwise, all the tests
        // should clean up after themselves.
        // THIS IS IN   S E T U P   RATHER THAN   T E A R D O W N   because
        // ending a debugging session early prevents tearDown() from running.
        let deleteLeftOverFiles = {
            let fileManager = FileManager.default

//            let dirList: [String] = NSSearchPathForDirectoriesInDomains(FileManager.SearchPathDirectory.documentDirectory, FileManager.SearchPathDomainMask.userDomainMask, true) as [String]
//            let dirPath = dirList[0];
            let dirPath = self.dbDirectory.path

            let enumerator = fileManager.enumerator(atPath: dirPath)
            var filesToDelete = [String]()
            var filePathName: String? = nil
            while let file = enumerator?.nextObject() as? String {
                filePathName = dirPath + "/" + file
                do {
                    let attrs = try fileManager.attributesOfItem(atPath: filePathName!) as NSDictionary
                    if (attrs.fileType() == FileAttributeType.typeRegular.rawValue) {
                        filesToDelete.append(filePathName!)
                    }
                } catch let error as NSError {
                    print(error.description)
                }
            }
            // Now delete any files. We couldn't delete them while fileManager
            // was enumerating them.
            for file in filesToDelete {
                do {
                    try fileManager.removeItem(atPath: file)
                } catch let error as NSError {
                    print(error.description)
                }
            }
        }
        deleteLeftOverFiles()

        let cleanupStatementCache = {
            JetDatabaseStatementCache.sharedInstance.finishAll()
        }
        cleanupStatementCache()
    }

    override func tearDown() {
        super.tearDown()
    }

    // MARK: - Helper Functions

    func deleteDatabaseFile(_ database: JetDatabase, databaseName: String) {
        guard let filePathName = database.dbPath else { return }
        let fMgr = FileManager.default
        if fMgr.fileExists(atPath: filePathName) {
            // File exists, as it should. Clean it up
            do {
                try fMgr.removeItem(atPath: filePathName)
            } catch {
                XCTFail("Testing Note: " + filePathName + " was not deleted. Future tests may show false positive.")
            }
        } else {
            XCTFail("Cannot delete non-existing database File: " + filePathName)
        }
    }

    // MARK: - Tests

    func testDocumentDirectory() {
        let database = JetDatabase.shared

        let databaseName = "TestDatabase.db"
        do {
            try database.openDatabase(databaseName, inDirectory: dbDirectory)
        } catch {
            XCTFail("Should have been able to open \(dbDirectory)\(databaseName)")
        }

        guard let dirUrl = database.directoryURL else {
            XCTFail("database directory should have been set by openDatabase")
            deleteDatabaseFile(database, databaseName: databaseName)
            return
        }
        let dirName = dirUrl.path
        XCTAssert(dirName.hasSuffix(dirName), "documentDirectoryString should be a string ending with " + dirName)
    }

    func testOpenDb() throws {

        let databaseName = "TestDatabase.db"
        let key = "secret"

        let database = JetDatabase.shared
        try database.openDatabase(databaseName, inDirectory: dbDirectory, key: key)

        database.close();
        deleteDatabaseFile(database, databaseName: databaseName)
    }

    func testCreateEncryptedDB() throws {
        let createTableSQL = """
            CREATE TABLE Person (
                person_id INTEGER PRIMARY KEY,
                first_name TEXT
            )
            """

        let databaseName = "Test.db"
        let key = "secret"
        let database = JetDatabase.shared

        try database.openDatabase(databaseName, inDirectory: dbDirectory, key: key)
        try database.execute(createTableSQL)

        print("testDB: " + database.directoryURL!.appendingPathComponent(databaseName).path)

        database.close()

        do {
            try database.openDatabase(databaseName, inDirectory: dbDirectory, key: "bad stuff")
            let _: [[String: Any?]] = try database.queryForDictionaries("SELECT count(*) AS count FROM sqlite_master")
            XCTFail("Should not be able to open an encrypted database without the correct key.")
            database.close()
        } catch {

        }

        do {
            try database.openDatabase(databaseName, inDirectory: dbDirectory, key: key)
            let _: [[String: Any?]] = try database.queryForDictionaries("SELECT count(*) AS count FROM sqlite_master")
        } catch {
            XCTFail("Should not be able to open an encrypted database without the correct key.")
        }
        database.close()

        deleteDatabaseFile(database, databaseName: databaseName)
    }

    func testDeployDatabase() {
        let databaseName = "BundledTest.db"

        // Verify that the resource we are testing with acutally exists in the bundle.
        guard let _ = Bundle.module.url(forResource: databaseName, withExtension:nil) else {
            XCTFail("The file '" + databaseName + "' must exists as a resource in the test bundle.")
            return
        }

        let database = JetDatabase.shared
        let bundle = Bundle.module
        do {
            try database.deployDatabase(dbName: databaseName, toDirectory: dbDirectory, bundle: bundle)
        } catch {
            XCTFail("deployDatabase should not throw exception: \(error)")
        }

        do {
            try database.openDatabase(databaseName, inDirectory: dbDirectory)
            database.close()
        } catch {
            XCTFail("openDatabase should not have thrown exception: \(error)")
        }
        deleteDatabaseFile(database, databaseName: databaseName)
    }

    func testCreateTableWithSQL() throws {
        let createTableSQL = """
            CREATE TABLE Person (
                person_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                birth_date INTEGER,
                photo BLOB,
                bed_time INTEGER
            )
            """

        let databaseName = "Test.db"
        let database = JetDatabase.shared

        try database.openDatabase(databaseName, inDirectory: dbDirectory)
        try database.execute(createTableSQL)

        database.close()
        deleteDatabaseFile(database, databaseName: databaseName)
    }

    func testCreateStringFunction() throws {
        let createTableSQL = """
            CREATE TABLE Person (
                person_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT
            )
            """

        let databaseName = "Test.db"
        let database = JetDatabase.shared

        try database.openDatabase(databaseName, inDirectory: dbDirectory)

        database.createStringFunction(functionName: "FullName", argCount: 2) { arguments in
            let first = (arguments[0] as? String) ?? ""
            let last = (arguments[1] as? String) ?? ""
            return first + " " + last
        }

        try database.execute(createTableSQL)

        let firstName1:    String    = "Fred"
        let lastName1:    String    = "Flintstone"

        // Insert
        let insertSQL =    "INSERT INTO Person (first_name, last_name) VALUES (?,?)"
        try database.execute(insertSQL, params: [firstName1, lastName1])

        let sql = "select FullName(first_name, last_name) as full_name from Person"
        let resultRows: [JetDatabaseResultRow] = try database.queryForResultRows(sql)
        XCTAssertEqual(resultRows.count, 1)
        let columns = resultRows[0]

        XCTAssertEqual(columns[0] as? String, firstName1 + " " + lastName1)

        database.close()
        deleteDatabaseFile(database, databaseName: databaseName)
    }

    func testCreateIntFunction() throws {
        let createTableSQL = """
            CREATE TABLE Person (
                person_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                score_1 INTEGER,
                score_2 INTEGER,
                score_3 INTEGER
            )
            """

        let databaseName = "Test.db"
        let database = JetDatabase.shared

        try database.openDatabase(databaseName, inDirectory: dbDirectory)

        database.createInt64Function(functionName: "TotalScore", argCount: 3) { arguments in
            let first = (arguments[0] as? Int64) ?? 0
            let second = (arguments[1] as? Int64) ?? 0
            let third = (arguments[2] as? Int64) ?? 0
            return first + second + third
        }

        try database.execute(createTableSQL)

        let firstName1:    String    = "Fred"
        let lastName1:    String    = "Flintstone"
        let score1 = 10
        let score2 = 5
        let score3 = 3

        // Insert
        let insertSQL =    "INSERT INTO Person (first_name, last_name, score_1, score_2, score_3) VALUES (?,?,?,?,?)"
        try database.execute(insertSQL, params: [firstName1, lastName1, score1, score2, score3])

        let sql = "select TotalScore(score_1, score_2, score_3) as total from Person"
        let resultRows: [JetDatabaseResultRow] = try database.queryForResultRows(sql)
        XCTAssertEqual(resultRows.count, 1)
        let columns = resultRows[0]

        XCTAssertEqual(columns[0] as? Int64, 18)

        database.close()
        deleteDatabaseFile(database, databaseName: databaseName)
    }

    func testInsertUpdateSelect() throws {
        let databaseName = try createTableWithFredAndBarney()
        let persons = [fred, barney]

        let database = JetDatabase.shared

        // Select
        let selectSQL = "SELECT * FROM Person"
        let resultRows: [JetDatabaseResultRow] = try database.queryForResultRows(selectSQL)
        XCTAssertEqual(resultRows.count, 2)

        var index = 0
        for person in persons {
            let columns = resultRows[index]
            XCTAssertEqual(columns.count, 8)

            XCTAssertTrue(columns[0] is Int64)
            XCTAssertEqual(columns[1] as? String, person.firstName)
            XCTAssertEqual(columns[2] as? String, person.lastName)

            let time = columns[3] as? Double
            let returnedBday = Date(timeIntervalSince1970: Double(time!))
            XCTAssertEqual(returnedBday, person.birthdate)

            XCTAssertEqual(columns[4] as? Data, person.photo)

            let bedTime = columns[5] as? Int64
            let returnedBed = Date(timeIntervalSince1970: Double(bedTime!))
            XCTAssertEqual(returnedBed, person.bedTime)

            XCTAssertEqual(columns[6] as? Int64, person.numberOfFriends)

            XCTAssertEqual(columns[7] as? Double, person.gpa)

            index += 1
        }
        database.close();
        deleteDatabaseFile(database, databaseName: databaseName)
    }

    func testRowChangeHandler() throws {
        let databaseName = try createTableWithFredAndBarney()
        let database = JetDatabase.shared
        let expect = expectation(description: "Expecting callback from database")

        database.rowChangeHandler(userData: "Fred Flintstone Data") { (tableName, rowId, changeType) in
            XCTAssertEqual(tableName, "Person")
            XCTAssertEqual(rowId, 1)
            XCTAssertEqual(changeType, .update)
            let sql = "SELECT * FROM Person WHERE rowId = ?"
            if let rows = try? database.queryForDictionaries(sql, params: [rowId]) {
                print("ROWS:\n\(rows)")
            }
            expect.fulfill()
        }

        let updateSQL = "UPDATE Person SET first_name = ?, last_name = ? WHERE first_name = 'Fred'"
        let first = "Mr."
        let last = "SparkingRock"
        try database.execute(updateSQL, params: [first, last])

        waitForExpectations(timeout: 1) { error in
            if let error = error {
                XCTFail("Aborted waiting for timeout: \(error)")
            }
        }

        database.close();
        deleteDatabaseFile(database, databaseName: databaseName)
    }


    func testQueryAsDictionary() throws {
        let databaseName = try createTableWithFredAndBarney()
        let persons = [fred, barney]

        let database = JetDatabase.shared

        // Select
        let selectSQL = "SELECT * FROM Person"
        let dictionaries: [[String: Any?]] = try database.queryForDictionaries(selectSQL)
        XCTAssertEqual(dictionaries.count, 2)

        var index = 0
        for person in persons {
            let columns = dictionaries[index]
            XCTAssertEqual(columns.count, 8)

            XCTAssertTrue(columns["person_id"] is Int64)
            XCTAssertEqual(columns["first_name"] as? String, person.firstName)
            XCTAssertEqual(columns["last_name"] as? String, person.lastName)

            let time = columns["birth_date"] as? Double
            let returnedBday = Date(timeIntervalSince1970: Double(time!))
            XCTAssertEqual(returnedBday, person.birthdate)

            XCTAssertEqual(columns["photo"] as? Data, person.photo)

            let bedTime = columns["bed_time"] as? Int64
            let returnedBed = Date(timeIntervalSince1970: Double(bedTime!))
            XCTAssertEqual(returnedBed, person.bedTime)

            XCTAssertEqual(columns["number_friends"] as? Int64, person.numberOfFriends)

            XCTAssertEqual(columns["gpa"] as? Double, person.gpa)

            index += 1
        }
        database.close();
        deleteDatabaseFile(database, databaseName: databaseName)
    }
    
    private func setupPeople(_ people: [StoneAgePerson], databaseName: String) throws {
        let createTableSQL = """
            CREATE TABLE Person (
                person_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                score_1 INTEGER,
                score_2 INTEGER,
                score_3 INTEGER
            )
            """

        let database = JetDatabase.shared

        try database.openDatabase(databaseName, inDirectory: dbDirectory)
        try database.execute(createTableSQL)

        

        for person in people {
            let insertSQL =    "INSERT INTO Person (first_name, last_name, score_1, score_2, score_3) VALUES (?,?,?,?,?)"
            try database.execute(insertSQL, params: person.params)
        }
    }
    
    func testNext() throws {
        let databaseName = "Test.db"

        let people: [StoneAgePerson] = [
            StoneAgePerson(first: "Fred", last: "Flintstone", score1: 20, score2: 30, score3: 40),
            StoneAgePerson(first: "Wilma", last: "Flintstone", score1: 80, score2: 90, score3: 100),
            StoneAgePerson(first: "Barney", last: "Rubble", score1: 40, score2: 50, score3: 60),
            StoneAgePerson(first: "Betty", last: "Rubble", score1: 60, score2: 70, score3: 80),
            StoneAgePerson(first: "Pebbles", last: "Flintstone", score1: 65, score2: 75, score3: 85),
            StoneAgePerson(first: "Bambam", last: "Rubble", score1: 35, score2: 45, score3: 55),
        ]

        try setupPeople(people, databaseName: databaseName)
        let database = JetDatabase.shared
        
        let sql = "select * from Person ORDER BY person_id"
        let resultSet = try database.resultSet(for: sql)
        var rowDictionary = resultSet.next()

        var index: Int = 0
        while rowDictionary != nil {
            XCTAssertEqual(rowDictionary!["person_id"] as? Int64, Int64(index+1))
            XCTAssertEqual(rowDictionary!["first_name"] as? String, people[index].first)
            XCTAssertEqual(rowDictionary!["last_name"] as? String, people[index].last)
            XCTAssertEqual(rowDictionary!["score_1"] as? Int64, Int64(people[index].score1))
            XCTAssertEqual(rowDictionary!["score_2"] as? Int64, Int64(people[index].score2))
            XCTAssertEqual(rowDictionary!["score_3"] as? Int64, Int64(people[index].score3))
            rowDictionary = resultSet.next()
            index += 1
        }
        XCTAssertEqual(index, people.count)
        
        database.close()
        deleteDatabaseFile(database, databaseName: databaseName)
    }

    func testNextResultRow() throws {
        let databaseName = "Test.db"

        let people: [StoneAgePerson] = [
            StoneAgePerson(first: "Fred", last: "Flintstone", score1: 20, score2: 30, score3: 40),
            StoneAgePerson(first: "Wilma", last: "Flintstone", score1: 80, score2: 90, score3: 100),
            StoneAgePerson(first: "Barney", last: "Rubble", score1: 40, score2: 50, score3: 60),
            StoneAgePerson(first: "Betty", last: "Rubble", score1: 60, score2: 70, score3: 80),
            StoneAgePerson(first: "Pebbles", last: "Flintstone", score1: 65, score2: 75, score3: 85),
            StoneAgePerson(first: "Bambam", last: "Rubble", score1: 35, score2: 45, score3: 55),
        ]

        try setupPeople(people, databaseName: databaseName)
        let database = JetDatabase.shared
        
        let sql = "select * from Person ORDER BY person_id"
        let resultSet = try database.resultSet(for: sql)
        var row = resultSet.nextResultRow()

        var index: Int = 0
        while row != nil {
            XCTAssertEqual(row![0] as? Int64, Int64(index+1))
            XCTAssertEqual(row![1] as? String, people[index].first)
            XCTAssertEqual(row![2] as? String, people[index].last)
            XCTAssertEqual(row![3] as? Int64, Int64(people[index].score1))
            XCTAssertEqual(row![4] as? Int64, Int64(people[index].score2))
            XCTAssertEqual(row![5] as? Int64, Int64(people[index].score3))
            row = resultSet.nextResultRow()
            index += 1
        }
        XCTAssertEqual(index, people.count)
        
        database.close()
        deleteDatabaseFile(database, databaseName: databaseName)
    }

    
    func testEntitySetter() {
        let sVal = "This is a test"
        let iVal = Int(23)
        let dVal = Double(10.5)
        let nVal: Data = String("Testing testing 1 2 3 4 5").data(using: String.Encoding.utf8)!

        var obj = JetDatabaseResultColumn()

        obj.value = sVal
        XCTAssert(obj.type == .text)

        obj.value = iVal
        XCTAssert(obj.type == .int)

        obj.value = Int(iVal)
        XCTAssert(obj.type == .int)

        obj.value = Int32(iVal)
        XCTAssert(obj.type == .int)

        obj.value = dVal
        XCTAssert(obj.type == .double)

        obj.value = Float(dVal)
        XCTAssert(obj.type == .double)

        obj.value = nVal
        XCTAssert(obj.type == .blob)

        obj.value = nil
        XCTAssert(obj.type == .null)
    }

    func testColumnConversion() {
        var obj = JetDatabaseResultColumn()
        obj.name = "test_field"

        let textString    = "This is text"
        let textBool    = "true"
        let textNum1    = "123"
        let textNum2    = "123.321"
        let textNum3    = "9.0"
        let textData    = "Test Data. test data. TEST DATA. This is a test."
        let num1 : Int64    = Int64(123)
        let num2 : Int64    = Int64((textNum2 as NSString).intValue)
        let num3 : Int64    = Int64((textNum3 as NSString).intValue)
        let dub1: Double = Double(123)
        let dub2: Double = Double(123.321)
        let dub3: Double = Double(9.0)
        let float1: Float = Float(123)
        let float2: Float = Float(123.321)
        let float3: Float = Float(9.0)
        let data: Data = String(textData).data(using: String.Encoding.utf8)!

        obj.value = textString
        var result: Any? = obj.value(asType: String.self)
        XCTAssert(result is String)
        XCTAssertEqual(result as! String, textString)

        result = obj.value(asType: Bool.self)
        XCTAssert(result is Bool)
        XCTAssertEqual(result as! Bool, false)

        obj.value = textBool
        result = obj.value(asType: Bool.self)
        XCTAssert(result is Bool)
        XCTAssertEqual(result as! Bool, true)

        obj.value = textNum1
        result = obj.value(asType: Int.self)
        XCTAssert(result is Int)
        XCTAssertEqual(result as! Int, Int(num1))

        obj.value = textNum2
        result = obj.value(asType: Int32.self)
        XCTAssert(result is Int32)
        XCTAssertEqual(result as! Int32, Int32(num2))

        obj.value = textNum3
        result = obj.value(asType: Int64.self)
        XCTAssert(result is Int64)
        XCTAssertEqual(result as! Int64, Int64(num3))

        result = obj.value(asType: UInt.self)
        XCTAssert(result is UInt)
        XCTAssertEqual(result as! UInt, UInt(num3))

        obj.value = textNum1
        result = obj.value(asType: Double.self)
        XCTAssert(result is Double)
        XCTAssertEqual(result as! Double, dub1)

        obj.value = textNum2
        result = obj.value(asType: Double.self)
        XCTAssert(result is Double)
        XCTAssertEqual(result as! Double, dub2)

        obj.value = textNum3
        result = obj.value(asType: Double.self)
        XCTAssert(result is Double)
        XCTAssertEqual(result as! Double, dub3)

        obj.value = textNum1
        result = obj.value(asType: Float.self)
        XCTAssert(result is Float)
        XCTAssertEqual(result as! Float, float1)

        obj.value = textNum2
        result = obj.value(asType: Float.self)
        XCTAssert(result is Float)
        XCTAssertEqual(result as! Float, float2)

        obj.value = textNum3
        result = obj.value(asType: Float.self)
        XCTAssert(result is Float)
        XCTAssertEqual(result as! Float, float3)

        obj.value = textData
        result = obj.value(asType: Data.self)
        XCTAssert(result is NSData)
        XCTAssertEqual(result as! Data, data)
    }

    func testDoubleConversion() {
        var obj = JetDatabaseResultColumn()
        obj.name = "test_field"

        let dub1: Double = Double(123)
        obj.value = dub1
        var result: Any? = obj.value(asType: String.self)
        XCTAssert(result is String)
        XCTAssertEqual((result as? String), String(dub1))

        let dub2: Double = Double(123.321)
        obj.value = dub2
        result = obj.value(asType: String.self)
        XCTAssert(result is String)
        XCTAssertEqual((result as? String), String(dub2))

        obj.value = 0.0
        result = obj.value(asType: Bool.self)
        XCTAssert(result is Bool)
        XCTAssertEqual(result as! Bool, false)

        obj.value = 1.0
        result = obj.value(asType: Bool.self)
        XCTAssert(result is Bool)
        XCTAssertEqual(result as! Bool, true)

        obj.value = 10.0
        result = obj.value(asType: Int.self)
        XCTAssert(result is Int)
        XCTAssertEqual(result as! Int, 10)

        obj.value = 0.0
        result = obj.value(asType: Int.self)
        XCTAssert(result is Int)
        XCTAssertEqual(result as! Int, 0)

        obj.value = 10.9
        result = obj.value(asType: Int.self)
        XCTAssert(result is Int)
        XCTAssertEqual(result as! Int, 10)

        obj.value = 10.9
        result = obj.value(asType: Int32.self)
        XCTAssert(result is Int32)
        XCTAssertEqual(result as! Int32, 10)

        obj.value = 10.9
        result = obj.value(asType: Int64.self)
        XCTAssert(result is Int64)
        XCTAssertEqual(result as! Int64, 10)

        obj.value = 10.9
        result = obj.value(asType: UInt.self)
        XCTAssert(result is UInt)
        XCTAssertEqual(result as! UInt, 10)

        obj.value = 10.9
        result = obj.value(asType: Double.self)
        XCTAssert(result is Double)
        XCTAssertEqual(result as! Double, 10.9)

        obj.value = 10.9
        result = obj.value(asType: Float.self)
        XCTAssert(result is Float)
        XCTAssertEqual(result as! Float, 10.9)

        let date = dateMatchingMilliseconds(of: Date())
        obj.value = date.timeIntervalSince1970
        result = obj.value(asType: Date.self)
        XCTAssertNotNil(result)
        XCTAssertEqual((result as! Date).timeIntervalSince1970, date.timeIntervalSince1970)

        obj.value = 10.0
        result = obj.value(asType: Date.self)
        XCTAssertEqual((result as! Date).timeIntervalSince1970, obj.value as? Double)
    }

    func testIntConversion() {
        var obj = JetDatabaseResultColumn()
        obj.name = "test_field"

        var iVal: Int64 = Int64(123)
        obj.value = iVal
        var result: Any? = obj.value(asType: String.self)
        XCTAssert(result is String)
        XCTAssertEqual((result as! String), String(iVal))

        iVal = Int64(123)
        obj.value = iVal
        result = obj.value(asType: Bool.self)
        XCTAssert(result is Bool)
        XCTAssertEqual((result as! Bool), true)

        iVal = Int64(0)
        obj.value = iVal
        result = obj.value(asType: Bool.self)
        XCTAssert(result is Bool)
        XCTAssertEqual((result as! Bool), false)

        iVal = Int64(123)
        obj.value = iVal
        result = obj.value(asType: Int.self)
        XCTAssert(result is Int)
        XCTAssertEqual((result as! Int), Int(iVal))

        iVal = Int64(123)
        obj.value = iVal
        result = obj.value(asType: Int32.self)
        XCTAssert(result is Int32)
        XCTAssertEqual((result as! Int32), Int32(iVal))

        iVal = Int64(123)
        obj.value = iVal
        result = obj.value(asType: Int64.self)
        XCTAssert(result is Int64)
        XCTAssertEqual((result as! Int64), Int64(iVal))

        iVal = Int64(123)
        obj.value = iVal
        result = obj.value(asType: UInt.self)
        XCTAssert(result is UInt)
        XCTAssertEqual((result as! UInt), UInt(iVal))

        let iVal1 = Int(123)
        obj.value = iVal
        result = obj.value(asType: Double.self)
        XCTAssert(result is Double)
        XCTAssertEqual((result as! Double), Double(iVal1))

        iVal = Int64(123)
        obj.value = iVal
        result = obj.value(asType: Float.self)
        XCTAssert(result is Float)
        XCTAssertEqual((result as! Float), Float(iVal))

        let now = dateMatchingMilliseconds(of: Date())
        iVal = Int64(now.timeIntervalSince1970)
        obj.value = iVal
        result = obj.value(asType: Date.self)
        XCTAssertTrue(result is Date)
        XCTAssertEqual((result as! Date), now)

        iVal = Int64(123)
        obj.value = iVal
        result = obj.value(asType: Data.self)
        XCTAssert(result == nil, ".Int to .Data should return nil")
    }

    func testDBQueryBuilder() {
        let queryBuilder = JetDatabaseQueryBuilder()

        let table = "MyFirstTable t1"
        let col1 = "col1"
        let col2 = "col2"
        let col3 = "col3"
        let col3Dup = "col3"
        let joins = [
            "JOIN MySecondTable t2 ON t2.id = t1.id",
            "JOIN MyThirdTable t3 ON t3.id = t2.id",
            "JOIN MyFourthTable t4 ON t4.id = t3.id AND t4.score = ?"
        ]
        let joinBoundValues = [ "100" ]

        let conditions = [
            "t1.data = ?",
            "t2.data2 = ?",
            "t3.data3 IS NOT NULL AND t3.data3 LIKE '%?%'"
        ]
        let conditionBindings = ["1", "Michael D`Angelo", "data"]

        let groupBy = "t1.group"
        let orderBy = ["t1.last", "t1.first"]

        var joinIdx = 0
        for join in joins {
            if joinIdx == joinBoundValues.count - 1 {
                queryBuilder.addJoin(join, bindings: joinBoundValues)
            }
            else {
                queryBuilder.addJoin(join)
            }
            joinIdx += 1
        }

        queryBuilder.addSelectTable(table)
        queryBuilder.addGroupBy(groupBy)

        for order in orderBy {
            queryBuilder.addOrderBy(order)
        }

        var conditionIdx = 0
        for condition in conditions {
            queryBuilder.addCondition(condition, bindings: [conditionBindings[conditionIdx]])
            conditionIdx += 1
        }

        var sql = queryBuilder.sql
        let expectedSelect = "SELECT * FROM " + table + " "
        var expected: String = ""
        expected += joins.joined(separator: " ") + " "
        expected += "WHERE " + "(" + conditions.joined(separator: ") AND (") + ") "
        expected += "GROUP BY " + groupBy + " "
        expected += "ORDER BY " + orderBy.joined(separator: ",") + " "
        XCTAssertEqual(sql, expectedSelect + expected)

        queryBuilder.addColumns([col1, col2, col3, col3Dup])
        sql = queryBuilder.sql
        let newExpectedSelect = "SELECT \(col1),\(col2),\(col3) FROM " + table + " "
        XCTAssertEqual(sql, newExpectedSelect + expected)

        let actualBindings: [Any] = queryBuilder.bindings
        let expectedBindings: [Any] = joinBoundValues + conditionBindings
        XCTAssertEqual(expectedBindings.count, actualBindings.count)

        var idx = 0
        XCTAssertEqual(actualBindings.count, expectedBindings.count)
        for expectedBinding in expectedBindings {
            let actualBinding = actualBindings[idx]
            XCTAssertEqual(actualBinding as! String, expectedBinding as! String)
            idx += 1
        }
    }

    func testFinishAll() throws {
        let createTableSQL = """
            CREATE TABLE Person (
                person_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                birth_date DATE,
                photo BLOB,
                bed_time DATE,
                number_friends INTEGER,
                gpa FLOAT
            )
            """

        let databaseName = "Test.db"

        let database = JetDatabase.shared
        try database.openDatabase(databaseName, inDirectory: dbDirectory)
        try database.execute(createTableSQL)

        let insertSQL1 = "INSERT INTO Person (first_name, last_name, birth_date, photo, bed_time, number_friends, gpa) VALUES (?,?,?,?,?,?,?)"
        let selectSQL1 = "SELECT * FROM Person"
        let selectSQL2 = "SELECT * FROM Person where last_name = ?"

        let cache = JetDatabaseStatementCache.sharedInstance

        var stmt1: sqlite3_stmt_ptr
        var stmt2: sqlite3_stmt_ptr
        var stmt3: sqlite3_stmt_ptr
        let result1 = cache.compile(sql: insertSQL1, db: database, cacheSQL: true)
        let result2 = cache.compile(sql: selectSQL1, db: database, cacheSQL: true)
        let result3 = cache.compile(sql: selectSQL2, db: database, cacheSQL: true)
        switch result1 {
            case .success(let statement): stmt1 = statement
            case .failure: XCTFail("Statement should have compiled"); return
        }
        switch result2 {
            case .success(let statement): stmt2 = statement
            case .failure: XCTFail("Statement should have compiled"); return
        }
        switch result3 {
            case .success(let statement): stmt3 = statement
            case .failure: XCTFail("Statement should have compiled"); return
        }


        XCTAssert(stmt1 != ABP_DB_NIL_PTR, "Should have a valid statment")
        XCTAssert(stmt2 != ABP_DB_NIL_PTR, "Should have a valid statment")
        XCTAssert(stmt3 != ABP_DB_NIL_PTR, "Should have a valid statment")

        // 4 compiled statments: Create, Insert, 2 Select
        XCTAssertEqual(JetDatabaseStatementCache.sharedInstance.statements.count, 4)

        cache.finishSql(createTableSQL)
        XCTAssertEqual(JetDatabaseStatementCache.sharedInstance.statements.count, 3)

        cache.finishStatement(stmt1)
        XCTAssertEqual(JetDatabaseStatementCache.sharedInstance.statements.count, 2)

        cache.finishAll()
        XCTAssertEqual(JetDatabaseStatementCache.sharedInstance.statements.count, 0)

        database.close()
        deleteDatabaseFile(database, databaseName: databaseName)
    }

    func testTransactionClosureRollback() throws {
        let createTableSQL = """
            CREATE TABLE Person (
                person_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                birth_date DATE,
                bed_time DATE,
                number_friends INTEGER,
                gpa FLOAT
        )
        """

        let databaseName = "Test.db"

        let database = JetDatabase.shared
        try database.openDatabase(databaseName, inDirectory: dbDirectory)
        print("DB Path: \(database.dbPath!)")
        try database.execute(createTableSQL)

        let insertSQL = "INSERT INTO Person (first_name, last_name, birth_date, bed_time, number_friends, gpa) VALUES (?,?,?,?,?,?)"
        let firstName = "Fred"
        let lastName = "Flintstone"
        let birthdate = Date().addYears(-25)
        let bedTime = Date(timeIntervalSince1970: 0).addHours(20)
        let numberFriends = 53
        let gpa = 3.54
        try database.execute(insertSQL, params: [firstName, lastName, birthdate.timeIntervalSince1970, bedTime.timeIntervalSince1970, numberFriends, gpa])

        do {
            try database.transaction {
                let updateSQL = "UPDATE Person SET first_name = ?, last_name = ?"
                    let first = "no more fred"
                    let last = "no more flintstone"
                try database.execute(updateSQL, params: [first, last])
                let list: [JetDatabaseResultRow] = try database.queryForResultRows("SELECT * FROM Person")
                guard let record1 = list.first else { XCTFail("Should get a record back"); return }
                XCTAssertEqual(record1[1] as? String, first)
                XCTAssertEqual(record1[2] as? String, last)
                let badSQL = "UPDATE badsql SET first_name, last_name"
                try database.execute(badSQL)
            }
        } catch {
        }
        let list: [JetDatabaseResultRow] = try database.queryForResultRows("SELECT * FROM Person")
        guard let record1 = list.first else { XCTFail("Should get a record back"); return }
        XCTAssertEqual(record1[1] as? String, firstName)
        XCTAssertEqual(record1[2] as? String, lastName)
        deleteDatabaseFile(database, databaseName: databaseName)
    }

    func testNestedTransactionClosures() throws {
        let createTableSQL = """
            CREATE TABLE Person (
                person_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                birth_date DATE,
                bed_time DATE,
                number_friends INTEGER,
                gpa FLOAT
        )
        """

        let databaseName = "Test.db"

        let database = JetDatabase.shared
        try database.openDatabase(databaseName, inDirectory: dbDirectory)
        print("DB Path: \(database.dbPath!)")
        try database.execute(createTableSQL)

        let insertSQL = "INSERT INTO Person (first_name, last_name, birth_date, bed_time, number_friends, gpa) VALUES (?,?,?,?,?,?)"
        let firstName = "Fred"
        let lastName = "Flintstone"
        let birthdate = Date().addYears(-25)
        let bedTime = Date(timeIntervalSince1970: 0).addHours(20)
        let numberFriends = 53
        let gpa = 3.54
        try database.execute(insertSQL, params: [firstName, lastName, birthdate.timeIntervalSince1970, bedTime.timeIntervalSince1970, numberFriends, gpa])

        do {
            try database.transaction {
                let updateSQL = "UPDATE Person SET first_name = ?, last_name = ?"
                let first = "no more fred"
                let last = "no more flintstone"
                try database.execute(updateSQL, params: [first, last])
                let list: [JetDatabaseResultRow] = try database.queryForResultRows("SELECT * FROM Person")
                guard let record1 = list.first else { XCTFail("Should get a record back"); return }
                XCTAssertEqual(record1[1] as? String, first)
                XCTAssertEqual(record1[2] as? String, last)

                // 2nd nesting
                try database.transaction {
                    let updateSQL = "UPDATE Person SET first_name = ?, last_name = ?"
                    let first = "NoFirsName"
                    let last = "noLastName"
                    try database.execute(updateSQL, params: [first, last])
                    let list: [JetDatabaseResultRow] = try database.queryForResultRows("SELECT * FROM Person")
                    guard let record1 = list.first else { XCTFail("Should get a record back"); return }
                    XCTAssertEqual(record1[1] as? String, first)
                    XCTAssertEqual(record1[2] as? String, last)

                    // 3rd nesting
                    try database.transaction {
                        let badSQL = "UPDATE badsql SET first_name, last_name"
                        try database.execute(badSQL)
                    }
                }
            }
        } catch {
        }

        // Test for original record after the rollback that was caused by failed sql.
        let list: [JetDatabaseResultRow] = try database.queryForResultRows("SELECT * FROM Person")
        guard let record1 = list.first else { XCTFail("Should get a record back"); return }
        XCTAssertEqual(record1[1] as? String, firstName)
        XCTAssertEqual(record1[2] as? String, lastName)
        deleteDatabaseFile(database, databaseName: databaseName)
    }

    func testTransaction() throws {
        let createTableSQL = """
            CREATE TABLE Person (
                person_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                birth_date DATE,
                bed_time DATE,
                number_friends INTEGER,
                gpa FLOAT
        )
        """

        let databaseName = "Test.db"

        let database = JetDatabase.shared
        try database.openDatabase(databaseName, inDirectory: dbDirectory)
        print("DB Path: \(database.dbPath!)")
        try database.execute(createTableSQL)

        let insertSQL = "INSERT INTO Person (first_name, last_name, birth_date, bed_time, number_friends, gpa) VALUES (?,?,?,?,?,?)"
        let firstName = "Fred"
        let lastName = "Flintstone"
        let birthdate = Date().addYears(-25)
        let bedTime = Date(timeIntervalSince1970: 0).addHours(20)
        let numberFriends = 53
        let gpa = 3.54
        try database.execute(insertSQL, params: [firstName, lastName, birthdate.timeIntervalSince1970, bedTime.timeIntervalSince1970, numberFriends, gpa])

        let transId = database.transactionBegin()
        let updateSQL = "UPDATE Person SET first_name = ?, last_name = ?"
        let first = "no more fred"
        let last = "no more flintstone"
        try database.execute(updateSQL, params: [first, last])
        database.TransactionEnd(transactionId: transId)
        
        let list: [JetDatabaseResultRow] = try database.queryForResultRows("SELECT * FROM Person")
        guard let record1 = list.first else { XCTFail("Should get a record back"); return }
        XCTAssertEqual(record1[1] as? String, first)
        XCTAssertEqual(record1[2] as? String, last)
        deleteDatabaseFile(database, databaseName: databaseName)
    }


    func testTransactionAbort() throws {
        let createTableSQL = """
            CREATE TABLE Person (
                person_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                birth_date DATE,
                bed_time DATE,
                number_friends INTEGER,
                gpa FLOAT
        )
        """

        let databaseName = "Test.db"

        let database = JetDatabase.shared
        try database.openDatabase(databaseName, inDirectory: dbDirectory)
        print("DB Path: \(database.dbPath!)")
        try database.execute(createTableSQL)

        let insertSQL = "INSERT INTO Person (first_name, last_name, birth_date, bed_time, number_friends, gpa) VALUES (?,?,?,?,?,?)"
        let firstName = "Fred"
        let lastName = "Flintstone"
        let birthdate = Date().addYears(-25)
        let bedTime = Date(timeIntervalSince1970: 0).addHours(20)
        let numberFriends = 53
        let gpa = 3.54
        try database.execute(insertSQL, params: [firstName, lastName, birthdate.timeIntervalSince1970, bedTime.timeIntervalSince1970, numberFriends, gpa])

        let transId = database.transactionBegin()
        let updateSQL = "UPDATE Person SET first_name = ?, last_name = ?"
        let first = "no more fred"
        let last = "no more flintstone"
        try database.execute(updateSQL, params: [first, last])

        let list: [JetDatabaseResultRow] = try database.queryForResultRows("SELECT * FROM Person")
        guard let record1 = list.first else { XCTFail("Should get a record back"); return }
        XCTAssertEqual(record1[1] as? String, first)
        XCTAssertEqual(record1[2] as? String, last)
        let badSQL = "UPDATE badsql SET first_name, last_name"
        do {
            try database.execute(badSQL)
            XCTFail("Should not succeed executing bad SQL")
        } catch {
            do {
                try database.transactionAbort(transactionId: transId, error: error)
            } catch {
                // This error is expected from transactionAbort. Test is working
            }
        }

        let afterList: [JetDatabaseResultRow] = try database.queryForResultRows("SELECT * FROM Person")
        guard let record1 = afterList.first else { XCTFail("Should get a record back"); return }
        XCTAssertEqual(record1[1] as? String, firstName)
        XCTAssertEqual(record1[2] as? String, lastName)
        deleteDatabaseFile(database, databaseName: databaseName)
    }

    func testQueryIteratorPerformance() throws {
        let createTableSQL = """
            CREATE TABLE Person (
                person_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                birth_date DATE,
                bed_time DATE,
                number_friends INTEGER,
                gpa FLOAT
            )
            """

        let databaseName = "Test.db"

        let database = JetDatabase.shared
        try database.openDatabase(databaseName, inDirectory: dbDirectory)
        try database.execute(createTableSQL)

        let insertSQL = "INSERT INTO Person (first_name, last_name, birth_date, bed_time, number_friends, gpa) VALUES (?,?,?,?,?,?)"
        let firstName = "Fred"
        let lastName = "Flintstone"
        let birthdate = Date().addYears(-25)
        let bedTime = Date(timeIntervalSince1970: 0).addHours(20)
        let numberFriends = 53
        let gpa = 3.54

        let count = 1000
        var insertCount = 0
        measure {
            do {
                try database.transaction {
                    for _ in 0..<count {
                        let first = firstName + String(insertCount)
                        let last = lastName + String (insertCount)
                        try database.execute(insertSQL, params: [first, last, birthdate.timeIntervalSince1970, bedTime.timeIntervalSince1970, numberFriends, gpa])
                        insertCount += 1
                    }
                }
                var resultCount = 0
                let resultSet: [JetDatabaseResultRow] = try database.queryForResultRows("SELECT * from Person")
                for row in resultSet {
                    XCTAssertNotNil(row[0] as? Int64)
                    XCTAssertEqual(row[1] as? String, firstName + String(resultCount))
                    XCTAssertNotNil(row[2] as? String)
                    XCTAssertNotNil(Date(timeIntervalSince1970: row[3] as! Double))
                    XCTAssertEqual(Date(timeIntervalSince1970: row[3] as! Double), birthdate)
                    XCTAssertNotNil(Date(timeIntervalSince1970: TimeInterval(row[4] as! Int64)))
                    XCTAssertEqual(Date(timeIntervalSince1970: TimeInterval(row[4] as! Int64)), bedTime)
                    XCTAssertNotNil(row[5] as? Int64)
                    XCTAssertNotNil(row[6] as? Double)
                    resultCount += 1
                }
                XCTAssertEqual(resultCount, insertCount)
            } catch {
                XCTFail("Unexpected exception")
            }
        }

        database.close()
        deleteDatabaseFile(database, databaseName: databaseName)
    }
}

private extension JetDatabaseCipherTests {

    func createTableWithFredAndBarney() throws -> String {
        let createTableSQL = """
            CREATE TABLE Person (
                person_id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                birth_date DATE,
                photo BLOB,
                bed_time DATE,
                number_friends INTEGER,
                gpa FLOAT
            )
            """

        let databaseName = "Test.db"

        let database = JetDatabase.shared
        do {
            //            try database.openDatabase(databaseName)
            let key = "secret"
            try database.openDatabase(databaseName, inDirectory: dbDirectory, key: key)
        }
        catch {
            XCTFail("Should be able to open (create) a non existant database.")
            return databaseName
        }

        try database.execute(createTableSQL)

        var person = fred
        let insertSQL =    "INSERT INTO Person ( first_name, last_name, birth_date, photo, bed_time, number_friends, gpa) VALUES (?,?,?,?,?,?,?)"

        try database.execute(insertSQL, params: [person.firstName,
                                                 person.lastName,
                                                 person.birthdate,
                                                 person.photo,
                                                 person.bedTime,
                                                 person.numberOfFriends,
                                                 person.gpa])

        var stmt: sqlite3_stmt_ptr
        var stmt2: sqlite3_stmt_ptr
        let result = JetDatabaseStatementCache.sharedInstance.compile(sql: insertSQL, db: database, cacheSQL: true)
        let result2 = JetDatabaseStatementCache.sharedInstance.compile(sql: insertSQL, db: database, cacheSQL: true)
        switch result {
            case .success(let statement): stmt = statement
            case .failure: XCTFail("Statement should have compiled"); return databaseName
        }
        switch result2 {
            case .success(let statement): stmt2 = statement
            case .failure: XCTFail("Statement should have compiled"); return databaseName
        }
        XCTAssert(stmt == stmt2, "Retrieving the statment from the cache should always result with the same statement")

        // Reuse SQL statement
        person = barney
        try database.execute(insertSQL, params: [person.firstName,
                                                 person.lastName,
                                                 person.birthdate,
                                                 person.photo,
                                                 person.bedTime,
                                                 person.numberOfFriends,
                                                 person.gpa])

        JetDatabaseStatementCache.sharedInstance.finishStatement(stmt)
        JetDatabaseStatementCache.sharedInstance.finishSql(createTableSQL)
        return databaseName
    }

    func dateMatchingMilliseconds(of date: Date) -> Date {
        // basically truncate any fractions of seconds.
        let nowMillis = Double(Int64(date.timeIntervalSince1970) * 1000)
        return Date(timeIntervalSince1970: nowMillis / 1000)
    }
}

extension Date {
    static func dateFromMilliseconds(_ seconds: UInt) -> Date {
        return Date(timeIntervalSince1970: Double(seconds))
    }

    var milliseconds: Int64 {
        return Int64(self.timeIntervalSince1970) * 1000
    }
}
