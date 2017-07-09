import XCTest
@testable import SSDPClient

let timeout: TimeInterval = 5

class SSDPDiscoveryTests: XCTestCase {
    let client = SSDPDiscovery()

    var discoverServiceExpectation: XCTestExpectation?
    var startExpectation: XCTestExpectation?
    var stopExpectation: XCTestExpectation?
    var errorExpectation: XCTestExpectation?

    override func setUp() {
        super.setUp()

        self.errorExpectation = expectation(description: "Error")
        self.errorExpectation!.isInverted = true
        self.client.delegate = self
    }

    override func tearDown() {
        super.tearDown()
    }

    func testDiscoverService() {
        self.startExpectation = expectation(description: "Start")
        self.discoverServiceExpectation = expectation(description: "DiscoverService")

        self.client.discoverService(searchTarget: "ssdp:all", timeout: timeout)

        wait(for: [self.errorExpectation!, self.startExpectation!, self.discoverServiceExpectation!], timeout: timeout)
    }

    func testStop() {
        self.stopExpectation = expectation(description: "Stop")
        self.client.discoverService()
        self.client.stop()
        wait(for: [self.errorExpectation!, self.stopExpectation!], timeout: timeout)
    }

    static var allTests = [
        ("testDiscoverService", testDiscoverService),
        ("testStop", testStop),
    ]
}

extension SSDPDiscoveryTests: SSDPDiscoveryDelegate {
    func ssdpDiscovery(_ discovery: SSDPDiscovery, didDiscoverService service: SSDPService) {
        self.discoverServiceExpectation?.fulfill()
        self.discoverServiceExpectation = nil
    }

    func ssdpDiscoveryDidStart(_ discovery: SSDPDiscovery) {
        self.startExpectation?.fulfill()
    }

    func ssdpDiscoveryDidFinish(_ discovery: SSDPDiscovery) {
        self.stopExpectation?.fulfill()
    }

    func ssdpDiscovery(_ discovery: SSDPDiscovery, didFinishWithError error: Error) {
        self.errorExpectation?.fulfill()
    }
}
