#!/usr/bin/env swift
import Contacts
import Foundation

let store = CNContactStore()
let semaphore = DispatchSemaphore(value: 0)
store.requestAccess(for: .contacts) { granted, error in
    if let error { FileHandle.standardError.write(Data("access error: \(error)\n".utf8)) }
    if !granted { fputs("Contacts access was not granted\n", stderr); exit(1) }
    semaphore.signal()
}
semaphore.wait()

let request = CNContactFetchRequest(keysToFetch: [
    CNContactVCardSerialization.descriptorForRequiredKeys(),
])
var contacts: [CNContact] = []
do {
    try store.enumerateContacts(with: request) { contact, _ in
        contacts.append(contact)
    }
    let data = try CNContactVCardSerialization.data(with: contacts)
    FileHandle.standardOutput.write(data)
    FileHandle.standardError.write(Data("exported \(contacts.count) contacts\n".utf8))
} catch {
    fputs("export error: \(error)\n", stderr)
    exit(1)
}
