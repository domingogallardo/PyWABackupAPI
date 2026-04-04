// swift-tools-version: 5.8

import PackageDescription

let package = Package(
    name: "swift_wa_oracle",
    platforms: [
        .macOS(.v13)
    ],
    dependencies: [
        .package(path: "../../../SwiftWABackupAPI")
    ],
    targets: [
        .executableTarget(
            name: "swift_wa_oracle",
            dependencies: [
                .product(name: "SwiftWABackupAPI", package: "SwiftWABackupAPI")
            ]
        )
    ]
)
