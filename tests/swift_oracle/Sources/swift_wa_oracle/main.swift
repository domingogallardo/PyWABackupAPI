import Foundation
import SwiftWABackupAPI

enum OracleError: Error {
    case invalidArguments
    case backupNotFound
}

func makeEncoder() -> JSONEncoder {
    let encoder = JSONEncoder()
    encoder.dateEncodingStrategy = .iso8601
    encoder.outputFormatting = [.sortedKeys]
    return encoder
}

func resolveBackup(root: String, identifier: String) throws -> (WABackup, IPhoneBackup) {
    let waBackup = WABackup(backupPath: root)
    let backups = try waBackup.getBackups()

    let backup: IPhoneBackup?
    if identifier == "-" {
        backup = backups.validBackups.first
    } else {
        backup = backups.validBackups.first(where: { $0.identifier == identifier })
    }

    guard let backup else {
        throw OracleError.backupNotFound
    }

    try waBackup.connectChatStorageDb(from: backup)
    return (waBackup, backup)
}

@main
struct Main {
    static func main() throws {
        let arguments = CommandLine.arguments
        guard arguments.count >= 4 else {
            throw OracleError.invalidArguments
        }

        let command = arguments[1]
        let root = arguments[2]
        let identifier = arguments[3]
        let (waBackup, _) = try resolveBackup(root: root, identifier: identifier)
        let encoder = makeEncoder()

        switch command {
        case "list-chats":
            let chats = try waBackup.getChats()
            let data = try encoder.encode(chats)
            FileHandle.standardOutput.write(data)

        case "get-chat":
            guard arguments.count >= 5, let chatId = Int(arguments[4]) else {
                throw OracleError.invalidArguments
            }
            let payload = try waBackup.getChat(chatId: chatId, directoryToSaveMedia: nil)
            let data = try encoder.encode(payload)
            FileHandle.standardOutput.write(data)

        default:
            throw OracleError.invalidArguments
        }
    }
}
