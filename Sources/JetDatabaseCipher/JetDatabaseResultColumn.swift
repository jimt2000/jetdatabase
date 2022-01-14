//
//  JetDatabaseResultColumn.swift
//  JETDataBase
//
//  Created by Jim Thomas on 2/26/19.
//

import Foundation

enum SqlType {
	case text
	case double
	case int
	case blob
	case null
}


extension String {
	fileprivate func iso8601Date() -> Date? {
		return JetDatabaseResultColumn.iso8601DateFormatter.date(from: self)
	}
}

extension Double {
	fileprivate func dateFromSeconds() -> Date {
		// Trim off fractions of seconds
		let seconds = Int64(self)
		return Date(timeIntervalSince1970: Double(seconds))
	}
}

extension Date {
	fileprivate static func dateFromSeconds<T: BinaryInteger>(_ seconds: T) -> Date {
		return Date(timeIntervalSince1970: Double(seconds))
	}
}


//-------------------------------------------------------------------------
/// An Object to represent a value of a column in a row.
struct JetDatabaseResultColumn {

	fileprivate static var _iso8601DateFormatter: DateFormatter?
	fileprivate static var iso8601DateFormatter: DateFormatter {
		guard let df = _iso8601DateFormatter else {
			let df = DateFormatter()
			df.dateFormat = "YYYY-MM-dd HH:mm.ss"
			JetDatabaseResultColumn._iso8601DateFormatter = df
			return df
		}
		return df
	}


	//-------------------------------------------------------------------------
	///
	fileprivate var _type: SqlType?
	var type: SqlType? { return _type }
	var name: String = ""

	fileprivate var _value: Any?
	/// Supported Types are: String, Double, Int64, Double, Data.
	var value: Any?  {
		get { return _value }
		set {
			if (newValue == nil) {
				_type = .null
			} else {
				switch newValue {
					case is String:
						_type = .text
						_value = newValue
					case is Double:
						_type = .double
						_value = newValue
					case is Float:
						_type = .double
						_value = Double(newValue as! Float)
					case is Int64:
						_type = .int
						_value = newValue
					case is Int32:
						_type = .int
						_value = Int64(newValue as! Int32)
					case is Int:
						_type = .int
						_value = Int64(newValue as! Int)
					case is UInt:
						_type = .int
						_value = Int64(newValue as! UInt)
					case is Data:
						_type = .blob
						_value = newValue

					default:
						assert(false, "\(#file):\(#line) DBEntityObject value must be String, Double, Int64, NSData or nil. (Int and Int32 are converted to Int64. Float is converted to Double)")
				}
			}
		}
	}

	//-------------------------------------------------------------------------
	/// Generates a Swift type for the Entity Object. The returned value can
	/// be safely cast to the Swift type that matches the requested DataType
	/// - Parameter dt: The enumertation for the swift object that should be
	///					generated.
	/// - Returns: A Swift type'd object
	func value<T>(asType: T.Type) -> T? {

		if let type = self.type {
			switch type {
			case SqlType.text:
				return convertTextEntityToDataType(asType)
			case SqlType.double:
				return convertDoubleEntityToDataType(asType)
			case SqlType.int:
				return convertIntEntityToDataType(asType)
			case SqlType.blob:
				return convertBlobEntityToDataType() as? T
			case SqlType.null:
				return nil
			}
		}
		return nil
	}

	//-------------------------------------------------------------------------
	///
	fileprivate func convertTextEntityToDataType<T>(_ dt: T.Type) -> T? {
		guard self.value != nil else { return nil }

		let value: String = self.value as? String ?? ""
		switch dt {
		case is String.Type:
			return value as? T
		case is Bool.Type:
				let val = (value).lowercased()
				return (val == "true" || val == "yes" || val == "1") as? T
		case is Int.Type:    return Int(value) as? T
		case is Int32.Type:  return Int32(Double(value) ?? 0) as? T
		case is Int64.Type:  return Int64(Double(value) ?? 0) as? T
		case is UInt.Type:   return UInt(Double(value) ?? 0) as? T
		case is Double.Type: return Double(value) as? T
		case is Float.Type:	 return Float(value) as? T
		case is Date.Type:   return value.iso8601Date() as? T
		case is Data.Type:   return value.data(using: String.Encoding.utf8) as? T
		default: return nil
		}
	}

	//-------------------------------------------------------------------------
	///
	fileprivate func convertDoubleEntityToDataType<T>(_ dt: T.Type) -> T? {
		guard self.value != nil else { return nil }

		let value = self.value as? Double ?? 0
		switch dt {
			case is String.Type: return String(value) as? T
			case is Bool.Type:   return (value != 0) as? T
			case is Int.Type:    return Int(floor(value)) as? T
			case is Int32.Type:  return Int32(floor(value)) as? T
			case is Int64.Type:  return Int64(floor(value)) as? T
			case is UInt.Type:   return UInt(value) as? T
			case is Double.Type: return Double(value) as? T
			case is Float.Type:  return Float(value) as? T
			case is Date.Type:   return value == 0 ? nil : value.dateFromSeconds() as? T
			case is Data.Type:   return nil
			default:             return nil
		}
	}

	//-------------------------------------------------------------------------
	///
	fileprivate func convertIntEntityToDataType<T>(_ dt: T.Type) -> T? {
		guard self.value != nil else { return nil }

		let value = self.value as? Int64 ?? 0
		switch dt {
			case is String.Type: return String(describing: value) as? T
			case is Bool.Type:   return (value != 0) as? T
			case is Int.Type:    return Int(value) as? T
			case is Int32.Type:  return Int32(value) as? T
			case is Int64.Type:  return value as? T
			case is UInt.Type:   return UInt(value) as? T
			case is Double.Type: return Double(value) as? T
			case is Float.Type:  return Float(value) as? T
			case is Date.Type:   return Date.dateFromSeconds(value) as? T
			case is Data.Type:   return nil
			default:             return nil
		}
	}

	//-------------------------------------------------------------------------
	///
	fileprivate func convertBlobEntityToDataType() -> Data? {
		return self.value as? Data
	}
}
