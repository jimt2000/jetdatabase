//
//  Date+Extension.swift
//  JETDataBase
//
//  Created by Jim Thomas on 2/26/19.
//

import Foundation

extension Date {
	func addWeeks(_ weeks: Int) -> Date {
		return addDays(7 * weeks)
	}

	func addDays(_ days: Int) -> Date {
		return Calendar.current.date(byAdding: .day, value: days, to: self)!
	}

	func addYears(_ years: Int) -> Date {
		return Calendar.current.date(byAdding: .year, value: years, to: self)!
	}

	func addHours(_ hours: Int) -> Date {
		return Calendar.current.date(byAdding: .hour, value: hours, to: self)!
	}

	func addMinutes(_ minutes: Int) -> Date {
		return Calendar.current.date(byAdding: .minute, value: minutes, to: self)!
	}

	func addSeconds(_ seconds: Int) -> Date {
		return Calendar.current.date(byAdding: .second, value: seconds, to: self)!
	}

	func addSeconds(_ seconds: TimeInterval) -> Date {
		return self.addSeconds(Int(seconds))
	}
}
