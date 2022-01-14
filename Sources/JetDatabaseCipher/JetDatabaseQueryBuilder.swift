//
//  DBQueryBuilder.swift
//  JETDataBase
//
//  Created by Jim Thomas on 2/26/19.
//

class JetDatabaseQueryBuilder {

	struct ClauseBinding {
		var sqlStrings: [String] = []
		var binding: [Any] = []
	}

	fileprivate var selectTable: ClauseBinding = ClauseBinding()
	fileprivate var columns: ClauseBinding = ClauseBinding()
	fileprivate var joins: ClauseBinding = ClauseBinding()
	fileprivate var conditions: ClauseBinding = ClauseBinding()
	fileprivate var groupBy: ClauseBinding = ClauseBinding()
	fileprivate var orderBy: ClauseBinding = ClauseBinding()

	public var sql: String {
		var sql = ""
		let columnList = columns.sqlStrings.count == 0 ? "*" : columns.sqlStrings.joined(separator: ",")
		let tableList = selectTable.sqlStrings.count == 0 ? "" : selectTable.sqlStrings.joined(separator: ",")
		if selectTable.sqlStrings.count > 0 {
			sql += "SELECT " + columnList + " FROM " + tableList + " "
		}
		sql += joins.sqlStrings.count > 0 ? (joins.sqlStrings.joined(separator: " ") + " ") : ""
		sql += conditions.sqlStrings.count > 0 ? ("WHERE " + conditions.sqlStrings.joined(separator: " AND ") + " ") : ""
		sql += groupBy.sqlStrings.count > 0 ? ("GROUP BY " + groupBy.sqlStrings.joined(separator: ",") + " ") : ""
		sql += orderBy.sqlStrings.count > 0 ? ("ORDER BY " + orderBy.sqlStrings.joined(separator: ",") + " ") : ""
		return sql
	}

	public var bindings: [Any] {
		let boundData: [Any] =
			columns.binding +
			selectTable.binding +
			joins.binding +
			conditions.binding +
			groupBy.binding +
			orderBy.binding
		return boundData
	}

	public final func addColumns(_ columns: [String], bindings: [Any]? = nil) {
		for column in columns {
			if self.columns.sqlStrings.contains(column) == false {
				self.columns.sqlStrings.append(column)
			}
		}
		if let bindings = bindings {
			self.columns.binding += bindings
		}
	}

	public final func addSelectTable(_ name: String, bindings: [Any]? = nil) {
		self.selectTable.sqlStrings.append(name)
		if let bindings = bindings {
			self.selectTable.binding += bindings
		}
	}

	public func addJoin(_ clause: String, bindings: [Any]? = nil) {
		self.joins.sqlStrings.append(clause)
		if let bindings = bindings {
			self.joins.binding += bindings
		}
	}

	public func addCondition(_ clause: String, bindings: [Any]? = nil) {
		self.conditions.sqlStrings.append("(" + clause + ")")
		if let bindings = bindings {
			self.conditions.binding += bindings
		}
	}

	public func addGroupBy(_ clause: String, bindings: [Any]? = nil) {
		self.groupBy.sqlStrings.append(clause)
		if let bindings = bindings {
			self.groupBy.binding += bindings
		}
	}

	public func addOrderBy(_ clause: String, bindings: [Any]? = nil) {
		self.orderBy.sqlStrings.append(clause)
		if let bindings = bindings {
			self.orderBy.binding += bindings
		}
	}
}
