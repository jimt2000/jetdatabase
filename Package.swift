// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "JetDatabase",
    products: [
        .library(
            name: "JetDatabaseCipher",
            targets: ["JetDatabaseCipher"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        // .package(url: /* package url */, from: "1.0.0"),
        .package(name: "SQLCipher", url: "git@github.com:ICSEng/sqlcipher-ios", from: "0.1.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "JetDatabaseCipher",
            dependencies: ["SQLCipher"],
            cSettings: [
                .define("SQLITE_HAS_CODEC", to: "1"),
                .define("SQLITE_TEMP_STORE", to: "3"),      // Probably not necessary, but keeping as a reference
                .define("SQLCIPHER_CRYPTO_CC", to: nil),    // Probably not necessary, ...
                .define("NDEBUG", to: "1")                  // Probably not necessary, ...
            ],
            swiftSettings: [                                // Probably not necessary, ...
                .define("SQLITE_HAS_CODEC"),                // Probably not necessary, ...
            ]),                                             // Probably not necessary, ...
        .testTarget(
            name: "JetDatabaseCipherTests",
            dependencies: ["JetDatabaseCipher", "SQLCipher"],
            resources: [.process("Resources/")]
        )
  ]
)
