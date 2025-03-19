import Foundation
import HeliumLogger
import LoggerAPI
import Socket

public typealias SSDPServiceStream = AsyncThrowingStream<SSDPService, any Error>

public final actor SSDPDiscovery {
    private var sockets: [Socket] = []
    private var continuation: SSDPServiceStream.Continuation?

    public init() {}
    
    /**
     Discover SSDP services for a duration.
     - Parameters:
        - duration: The amount of time to wait
        - searchTarget: The type of the searched service
        - port: The port to use for discovery
        - onInterfaces: The network interfaces to use for discovery
     */
    public func discoverService(
        forDuration duration: TimeInterval = 10,
        searchTarget: String = "ssdp:all",
        port: Int32 = 1900,
        onInterfaces: [String?] = [nil]
    ) -> SSDPServiceStream {
        SSDPServiceStream { [weak self] continuation in
            continuation.onTermination = { [weak self] _ in
                Task {
                    await self?.stop()
                }
            }
            Task {
                guard let self else {
                    continuation.finish()
                    return
                }
                await self.set(continuation: continuation)
                await self.createSockets(
                    duration: duration,
                    searchTarget: searchTarget,
                    port: port,
                    onInterfaces: onInterfaces
                )
                await self.readResponses()
            }
            Task {
                try await Task.sleep(for: .seconds(duration))
                await self?.stop()
            }
        }
    }
    
    public func stop() {
        continuation?.finish()
        continuation = nil
        
        while sockets.isEmpty == false {
            sockets.removeLast().close()
        }
    }
}

private extension SSDPDiscovery {
    private func set(continuation: SSDPServiceStream.Continuation) {
        self.continuation = continuation
    }
    
    private func createSockets(
        duration: TimeInterval,
        searchTarget: String = "ssdp:all",
        port: Int32 = 1900,
        onInterfaces: [String?] = [nil]
    ) async {
        // Setup sockets for each interface
        for interface in onInterfaces {
            var socket: Socket? = nil
            do {
                let defaultAddress = "127.0.0.1"
                let addressToUse = interface ?? defaultAddress
                let interfaceAddr = Socket.createAddress(for: addressToUse, on: 0)
                let multicastAddr: String
                let family: Socket.ProtocolFamily
                
                if case .ipv6? = interfaceAddr {
                    multicastAddr = "ff02::c"
                    family = .inet6
                } else {
                    multicastAddr = "239.255.255.250"
                    family = .inet
                }
                socket = try Socket.create(family: family, type: .datagram, proto: .udp)
                
                guard let socket = socket else {
                    continue
                }
                try socket.listen(on: 0, node: interface)
                let message =
                "M-SEARCH * HTTP/1.1\r\n" + "MAN: \"ssdp:discover\"\r\n" + "HOST: \(multicastAddr):\(port)\r\n"
                + "ST: \(searchTarget)\r\n" + "MX: \(Int(duration))\r\n\r\n"
                try socket.write(from: message, to: Socket.createAddress(for: multicastAddr, on: port)!)
                sockets.append(socket)
            } catch {
                socket?.close()
                Log.info("Socket error: \(error) on interface \(interface ?? "default")")
            }
        }
    }
    
    /// Read responses from all sockets
    private func readResponses() async {
        for socket in sockets {
            do {
                var data = Data()
                let (bytesRead, address) = try socket.readDatagram(into: &data)
                
                guard
                    bytesRead > 0,
                    let response = String(data: data, encoding: .utf8),
                    let address = address,
                    let (remoteHost, _) = Socket.hostnameAndPort(from: address)
                else {
                    continue
                }
                Log.debug("Received: \(response) from \(remoteHost)")
                
                let service = SSDPService(host: remoteHost, response: response)
                continuation?.yield(service)
            } catch {
                Log.error("Socket error: \(error)")
            }
        }
    }
}
