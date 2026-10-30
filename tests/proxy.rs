use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tower_service::Service;

use hyper_util::client::legacy::connect::proxy::{
    SocksError, SocksReplyStage, SocksV4, SocksV5, Tunnel, TunnelError,
};
use hyper_util::client::legacy::connect::HttpConnector;

#[cfg(not(miri))]
#[tokio::test]
async fn test_tunnel_works() {
    let tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = tcp.local_addr().expect("local_addr");

    let proxy_dst = format!("http://{addr}").parse().expect("uri");
    let mut connector = Tunnel::new(proxy_dst, HttpConnector::new());
    let t1 = tokio::spawn(async move {
        let _conn = connector
            .call("https://hyper.rs".parse().unwrap())
            .await
            .expect("tunnel");
    });

    let t2 = tokio::spawn(async move {
        let (mut io, _) = tcp.accept().await.expect("accept");
        let mut buf = [0u8; 64];
        let n = io.read(&mut buf).await.expect("read 1");
        assert_eq!(
            &buf[..n],
            b"CONNECT hyper.rs:443 HTTP/1.1\r\nHost: hyper.rs:443\r\n\r\n"
        );
        io.write_all(b"HTTP/1.1 200 OK\r\n\r\n")
            .await
            .expect("write 1");
    });

    t1.await.expect("task 1");
    t2.await.expect("task 2");
}

#[cfg(not(miri))]
#[tokio::test]
async fn test_socks_v5_without_auth_works() {
    let proxy_tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy_addr = proxy_tcp.local_addr().expect("local_addr");
    let proxy_dst = format!("http://{proxy_addr}").parse().expect("uri");

    let target_tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let target_addr = target_tcp.local_addr().expect("local_addr");
    let target_dst = format!("http://{target_addr}").parse().expect("uri");

    let mut connector = SocksV5::new(proxy_dst, HttpConnector::new());

    // Client
    //
    // Will use `SocksV5` to establish proxy tunnel.
    // Will send "Hello World!" to the target and receive "Goodbye!" back.
    let t1 = tokio::spawn(async move {
        let conn = connector.call(target_dst).await.expect("tunnel");
        let mut tcp = conn.into_inner();

        tcp.write_all(b"Hello World!").await.expect("write 1");

        let mut buf = [0u8; 64];
        let n = tcp.read(&mut buf).await.expect("read 1");
        assert_eq!(&buf[..n], b"Goodbye!");
    });

    // Proxy
    //
    // Will receive CONNECT command from client.
    // Will connect to target and success code back to client.
    // Will blindly tunnel between client and target.
    let t2 = tokio::spawn(async move {
        let (mut to_client, _) = proxy_tcp.accept().await.expect("accept");
        let mut buf = [0u8; 513];

        // negotiation req/res
        let n = to_client.read(&mut buf).await.expect("read 1");
        assert_eq!(&buf[..n], [0x05, 0x01, 0x00]);

        to_client.write_all(&[0x05, 0x00]).await.expect("write 1");

        // command req/rs
        let [p1, p2] = target_addr.port().to_be_bytes();
        let [ip1, ip2, ip3, ip4] = [0x7f, 0x00, 0x00, 0x01];
        let message = [0x05, 0x01, 0x00, 0x01, ip1, ip2, ip3, ip4, p1, p2];
        let n = to_client.read(&mut buf).await.expect("read 2");
        assert_eq!(&buf[..n], message);

        let mut to_target = TcpStream::connect(target_addr).await.expect("connect");

        let message = [0x05, 0x00, 0x00, 0x01, ip1, ip2, ip3, ip4, p1, p2];
        to_client.write_all(&message).await.expect("write 2");

        let (from_client, from_target) =
            tokio::io::copy_bidirectional(&mut to_client, &mut to_target)
                .await
                .expect("proxy");

        assert_eq!(from_client, 12);
        assert_eq!(from_target, 8)
    });

    // Target server
    //
    // Will accept connection from proxy server
    // Will receive "Hello World!" from the client and return "Goodbye!"
    let t3 = tokio::spawn(async move {
        let (mut io, _) = target_tcp.accept().await.expect("accept");
        let mut buf = [0u8; 64];

        let n = io.read(&mut buf).await.expect("read 1");
        assert_eq!(&buf[..n], b"Hello World!");

        io.write_all(b"Goodbye!").await.expect("write 1");
    });

    t1.await.expect("task - client");
    t2.await.expect("task - proxy");
    t3.await.expect("task - target");
}

#[cfg(not(miri))]
#[tokio::test]
async fn test_socks_v5_with_auth_works() {
    let proxy_tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy_addr = proxy_tcp.local_addr().expect("local_addr");
    let proxy_dst = format!("http://{proxy_addr}").parse().expect("uri");

    let target_tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let target_addr = target_tcp.local_addr().expect("local_addr");
    let target_dst = format!("http://{target_addr}").parse().expect("uri");

    let mut connector =
        SocksV5::new(proxy_dst, HttpConnector::new()).with_auth("user".into(), "pass".into());

    // Client
    //
    // Will use `SocksV5` to establish proxy tunnel.
    // Will send "Hello World!" to the target and receive "Goodbye!" back.
    let t1 = tokio::spawn(async move {
        let conn = connector.call(target_dst).await.expect("tunnel");
        let mut tcp = conn.into_inner();

        tcp.write_all(b"Hello World!").await.expect("write 1");

        let mut buf = [0u8; 64];
        let n = tcp.read(&mut buf).await.expect("read 1");
        assert_eq!(&buf[..n], b"Goodbye!");
    });

    // Proxy
    //
    // Will receive CONNECT command from client.
    // Will connect to target and success code back to client.
    // Will blindly tunnel between client and target.
    let t2 = tokio::spawn(async move {
        let (mut to_client, _) = proxy_tcp.accept().await.expect("accept");
        let mut buf = [0u8; 513];

        // negotiation req/res
        let n = to_client.read(&mut buf).await.expect("read 1");
        assert_eq!(&buf[..n], [0x05, 0x01, 0x02]);

        to_client.write_all(&[0x05, 0x02]).await.expect("write 1");

        // auth req/res
        let n = to_client.read(&mut buf).await.expect("read 2");
        let [u1, u2, u3, u4] = b"user";
        let [p1, p2, p3, p4] = b"pass";
        let message = [0x01, 0x04, *u1, *u2, *u3, *u4, 0x04, *p1, *p2, *p3, *p4];
        assert_eq!(&buf[..n], message);

        to_client.write_all(&[0x01, 0x00]).await.expect("write 2");

        // command req/res
        let n = to_client.read(&mut buf).await.expect("read 3");
        let [p1, p2] = target_addr.port().to_be_bytes();
        let [ip1, ip2, ip3, ip4] = [0x7f, 0x00, 0x00, 0x01];
        let message = [0x05, 0x01, 0x00, 0x01, ip1, ip2, ip3, ip4, p1, p2];
        assert_eq!(&buf[..n], message);

        let mut to_target = TcpStream::connect(target_addr).await.expect("connect");

        let message = [0x05, 0x00, 0x00, 0x01, ip1, ip2, ip3, ip4, p1, p2];
        to_client.write_all(&message).await.expect("write 3");

        let (from_client, from_target) =
            tokio::io::copy_bidirectional(&mut to_client, &mut to_target)
                .await
                .expect("proxy");

        assert_eq!(from_client, 12);
        assert_eq!(from_target, 8)
    });

    // Target server
    //
    // Will accept connection from proxy server
    // Will receive "Hello World!" from the client and return "Goodbye!"
    let t3 = tokio::spawn(async move {
        let (mut io, _) = target_tcp.accept().await.expect("accept");
        let mut buf = [0u8; 64];

        let n = io.read(&mut buf).await.expect("read 1");
        assert_eq!(&buf[..n], b"Hello World!");

        io.write_all(b"Goodbye!").await.expect("write 1");
    });

    t1.await.expect("task - client");
    t2.await.expect("task - proxy");
    t3.await.expect("task - target");
}

#[cfg(not(miri))]
#[tokio::test]
async fn test_socks_v5_with_server_resolved_domain_works() {
    let proxy_tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy_addr = proxy_tcp.local_addr().expect("local_addr");
    let proxy_addr = format!("http://{proxy_addr}").parse().expect("uri");

    let mut connector = SocksV5::new(proxy_addr, HttpConnector::new())
        .with_auth("user".into(), "pass".into())
        .local_dns(false);

    // Client
    //
    // Will use `SocksV5` to establish proxy tunnel.
    // Will send "Hello World!" to the target and receive "Goodbye!" back.
    let t1 = tokio::spawn(async move {
        let _conn = connector
            .call("https://hyper.rs:443".try_into().unwrap())
            .await
            .expect("tunnel");
    });

    // Proxy
    //
    // Will receive CONNECT command from client.
    // Will connect to target and success code back to client.
    // Will blindly tunnel between client and target.
    let t2 = tokio::spawn(async move {
        let (mut to_client, _) = proxy_tcp.accept().await.expect("accept");
        let mut buf = [0u8; 513];

        // negotiation req/res
        let n = to_client.read(&mut buf).await.expect("read 1");
        assert_eq!(&buf[..n], [0x05, 0x01, 0x02]);

        to_client.write_all(&[0x05, 0x02]).await.expect("write 1");

        // auth req/res
        let n = to_client.read(&mut buf).await.expect("read 2");
        let [u1, u2, u3, u4] = b"user";
        let [p1, p2, p3, p4] = b"pass";
        let message = [0x01, 0x04, *u1, *u2, *u3, *u4, 0x04, *p1, *p2, *p3, *p4];
        assert_eq!(&buf[..n], message);

        to_client.write_all(&[0x01, 0x00]).await.expect("write 2");

        // command req/res
        let n = to_client.read(&mut buf).await.expect("read 3");

        let host = "hyper.rs";
        let port: u16 = 443;
        let mut message = vec![0x05, 0x01, 0x00, 0x03, host.len() as u8];
        message.extend(host.bytes());
        message.extend(port.to_be_bytes());
        assert_eq!(&buf[..n], message);

        let mut message = vec![0x05, 0x00, 0x00, 0x03, host.len() as u8];
        message.extend(host.bytes());
        message.extend(port.to_be_bytes());
        to_client.write_all(&message).await.expect("write 3");
    });

    t1.await.expect("task - client");
    t2.await.expect("task - proxy");
}

#[cfg(not(miri))]
#[tokio::test]
async fn test_socks_v5_with_locally_resolved_domain_works() {
    let proxy_tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy_addr = proxy_tcp.local_addr().expect("local_addr");
    let proxy_addr = format!("http://{proxy_addr}").parse().expect("uri");

    let mut connector = SocksV5::new(proxy_addr, HttpConnector::new())
        .with_auth("user".into(), "pass".into())
        .local_dns(true);

    // Client
    //
    // Will use `SocksV5` to establish proxy tunnel.
    // Will send "Hello World!" to the target and receive "Goodbye!" back.
    let t1 = tokio::spawn(async move {
        let _conn = connector
            .call("https://hyper.rs:443".try_into().unwrap())
            .await
            .expect("tunnel");
    });

    // Proxy
    //
    // Will receive CONNECT command from client.
    // Will connect to target and success code back to client.
    // Will blindly tunnel between client and target.
    let t2 = tokio::spawn(async move {
        let (mut to_client, _) = proxy_tcp.accept().await.expect("accept");
        let mut buf = [0u8; 513];

        // negotiation req/res
        let n = to_client.read(&mut buf).await.expect("read 1");
        assert_eq!(&buf[..n], [0x05, 0x01, 0x02]);

        to_client.write_all(&[0x05, 0x02]).await.expect("write 1");

        // auth req/res
        let n = to_client.read(&mut buf).await.expect("read 2");
        let [u1, u2, u3, u4] = b"user";
        let [p1, p2, p3, p4] = b"pass";
        let message = [0x01, 0x04, *u1, *u2, *u3, *u4, 0x04, *p1, *p2, *p3, *p4];
        assert_eq!(&buf[..n], message);

        to_client.write_all(&[0x01, 0x00]).await.expect("write 2");

        // command req/res
        let n = to_client.read(&mut buf).await.expect("read 3");
        let message = [0x05, 0x01, 0x00];
        assert_eq!(&buf[..3], message);
        assert!(buf[3] == 0x01 || buf[3] == 0x04); // IPv4 or IPv6
        assert_eq!(n, 4 + 4 * (buf[3] as usize) + 2);

        let message = vec![0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0];
        to_client.write_all(&message).await.expect("write 3");
    });

    t1.await.expect("task - client");
    t2.await.expect("task - proxy");
}

#[cfg(not(miri))]
#[tokio::test]
async fn test_socks_v4_works() {
    let proxy_tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy_addr = proxy_tcp.local_addr().expect("local_addr");
    let proxy_dst = format!("http://{proxy_addr}").parse().expect("uri");

    let target_tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let target_addr = target_tcp.local_addr().expect("local_addr");
    let target_dst = format!("http://{target_addr}").parse().expect("uri");

    let mut connector = SocksV4::new(proxy_dst, HttpConnector::new());

    // Client
    //
    // Will use `SocksV4` to establish proxy tunnel.
    // Will send "Hello World!" to the target and receive "Goodbye!" back.
    let t1 = tokio::spawn(async move {
        let conn = connector.call(target_dst).await.expect("tunnel");
        let mut tcp = conn.into_inner();

        tcp.write_all(b"Hello World!").await.expect("write 1");

        let mut buf = [0u8; 64];
        let n = tcp.read(&mut buf).await.expect("read 1");
        assert_eq!(&buf[..n], b"Goodbye!");
    });

    // Proxy
    //
    // Will receive CONNECT command from client.
    // Will connect to target and success code back to client.
    // Will blindly tunnel between client and target.
    let t2 = tokio::spawn(async move {
        let (mut to_client, _) = proxy_tcp.accept().await.expect("accept");
        let mut buf = [0u8; 512];

        let [p1, p2] = target_addr.port().to_be_bytes();
        let [ip1, ip2, ip3, ip4] = [127, 0, 0, 1];
        let message = [4, 0x01, p1, p2, ip1, ip2, ip3, ip4, 0, 0];
        let n = to_client.read(&mut buf).await.expect("read");
        assert_eq!(&buf[..n], message);

        let mut to_target = TcpStream::connect(target_addr).await.expect("connect");

        let message = [0, 90, p1, p2, ip1, ip2, ip3, ip4];
        to_client.write_all(&message).await.expect("write");

        let (from_client, from_target) =
            tokio::io::copy_bidirectional(&mut to_client, &mut to_target)
                .await
                .expect("proxy");

        assert_eq!(from_client, 12);
        assert_eq!(from_target, 8)
    });

    // Target server
    //
    // Will accept connection from proxy server
    // Will receive "Hello World!" from the client and return "Goodbye!"
    let t3 = tokio::spawn(async move {
        let (mut io, _) = target_tcp.accept().await.expect("accept");
        let mut buf = [0u8; 64];

        let n = io.read(&mut buf).await.expect("read 1");
        assert_eq!(&buf[..n], b"Hello World!");

        io.write_all(b"Goodbye!").await.expect("write 1");
    });

    t1.await.expect("task - client");
    t2.await.expect("task - proxy");
    t3.await.expect("task - target");
}

#[cfg(not(miri))]
#[tokio::test]
async fn test_socks_v5_optimistic_works() {
    let proxy_tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let proxy_addr = proxy_tcp.local_addr().expect("local_addr");
    let proxy_dst = format!("http://{proxy_addr}").parse().expect("uri");

    let target_addr = std::net::SocketAddr::new([127, 0, 0, 1].into(), 1234);
    let target_dst = format!("http://{target_addr}").parse().expect("uri");

    let mut connector = SocksV5::new(proxy_dst, HttpConnector::new())
        .with_auth("ABC".into(), "XYZ".into())
        .send_optimistically(true);

    // Client
    //
    // Will use `SocksV5` to establish proxy tunnel.
    // Will send "Hello World!" to the target and receive "Goodbye!" back.
    let t1 = tokio::spawn(async move {
        let _ = connector.call(target_dst).await.expect("tunnel");
    });

    // Proxy
    //
    // Will receive SOCKS handshake from client.
    // Will connect to target and success code back to client.
    // Will blindly tunnel between client and target.
    let t2 = tokio::spawn(async move {
        let (mut to_client, _) = proxy_tcp.accept().await.expect("accept");
        let [p1, p2] = target_addr.port().to_be_bytes();

        let mut buf = [0; 22];
        let request = vec![
            5, 1, 2, // Negotiation
            1, 3, 65, 66, 67, 3, 88, 89, 90, // Auth ("ABC"/"XYZ")
            5, 1, 0, 1, 127, 0, 0, 1, p1, p2, // Reply
        ];

        let response = vec![
            5, 2, // Negotiation,
            1, 0, // Auth,
            5, 0, 0, 1, 127, 0, 0, 1, p1, p2, // Reply
        ];

        // Accept all handshake messages
        to_client.read_exact(&mut buf).await.expect("read");
        assert_eq!(request.as_slice(), buf);

        // Send all handshake messages back
        to_client
            .write_all(response.as_slice())
            .await
            .expect("write");

        to_client.flush().await.expect("flush");
    });

    t1.await.expect("task - client");
    t2.await.expect("task - proxy");
}

#[tokio::test]
async fn fragmented_connect_preserves_bytes_following_the_response_header() {
    for split in [1, 8, 12, 17] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
            }
            let response = b"HTTP/1.1 200 OK\r\nX-Test: yes\r\n\r\nTUNNEL";
            stream.write_all(&response[..split]).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            stream.write_all(&response[split..]).await.unwrap();
        });
        let mut connector = Tunnel::new(proxy, HttpConnector::new());
        let connection = connector
            .call("https://origin.test".parse().unwrap())
            .await
            .unwrap();
        let mut stream = connection.into_inner();
        let mut bytes = [0; 6];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"TUNNEL");
        server.await.unwrap();
    }
}

#[tokio::test]
async fn an_invalid_connect_prefix_is_rejected_while_the_proxy_stays_open() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = format!("http://{}", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(stream.read_u8().await.unwrap());
        }
        stream.write_all(b"SSH-2.0-peer\r\n").await.unwrap();
        let mut rest = Vec::new();
        if let Err(error) = stream.read_to_end(&mut rest).await {
            // Early rejection may reset TCP while greeting bytes remain unread.
            assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
        }
        assert!(rest.is_empty());
    });
    let mut connector = Tunnel::new(proxy, HttpConnector::new());
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        connector.call("https://origin.test".parse().unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(
        result.err().unwrap().to_string(),
        "tunnel error: unsuccessful"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn final_connect_refusals_are_reported_while_the_proxy_stays_open() {
    for (status, expected) in [
        (
            "407 Proxy Authentication Required",
            "tunnel error: proxy authorization required",
        ),
        ("403 Forbidden", "tunnel error: unsuccessful"),
        ("099 Invalid", "tunnel error: unsuccessful"),
        ("101 Switching Protocols", "tunnel error: unsuccessful"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
            }
            // No terminating blank line and no EOF: only the refused status is known.
            stream
                .write_all(format!("HTTP/1.1 {status}\r\n").as_bytes())
                .await
                .unwrap();
            let mut rest = Vec::new();
            if let Err(error) = stream.read_to_end(&mut rest).await {
                assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
            }
            assert!(rest.is_empty());
        });
        let mut connector = Tunnel::new(proxy, HttpConnector::new());
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            connector.call("https://origin.test".parse().unwrap()),
        )
        .await
        .expect("a known final refusal must not await headers or EOF");
        let error = result.err().unwrap();
        assert_eq!(error.to_string(), expected);
        match error {
            TunnelError::Response {
                head,
                status: received,
                head_complete,
                ..
            } => {
                assert_eq!(received, status[..3].parse::<u16>().ok());
                assert!(!head_complete);
                assert!(format!("HTTP/1.1 {status}\r\n")
                    .as_bytes()
                    .starts_with(&head));
                assert!(!head.is_empty());
            }
            other => panic!("lost the native received status: {other}"),
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn failed_socks_exchanges_keep_consumed_reply_and_original_auth_status() {
    for authentication in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut greeting = [0; 3];
            stream.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting, [5, 1, if authentication { 2 } else { 0 }]);
            stream.write_all(&[5, greeting[2]]).await.unwrap();
            if authentication {
                let mut auth = [0; 5];
                stream.read_exact(&mut auth).await.unwrap();
                assert_eq!(&auth, &[1, 1, b'u', 1, b'p']);
                stream.write_all(&[1, 7]).await.unwrap();
            } else {
                // IPv4 command request, then a malformed address type followed
                // by bytes the parser never examines.
                let mut command = [0; 10];
                stream.read_exact(&mut command).await.unwrap();
                stream.write_all(&[5, 0, 0, 255, 9, 8, 7]).await.unwrap();
            }
        });
        let mut connector = SocksV5::new(proxy, HttpConnector::new());
        if authentication {
            connector = connector.with_auth("u".into(), "p".into());
        }
        let error = connector
            .call("http://127.0.0.1:80".parse().unwrap())
            .await
            .err()
            .unwrap();
        peer.await.unwrap();
        match error {
            SocksError::Response {
                stage,
                bytes,
                complete,
                ..
            } => {
                if authentication {
                    assert!(matches!(stage, SocksReplyStage::Authentication));
                    assert_eq!(bytes.as_ref(), &[1, 7]);
                    assert!(complete);
                } else {
                    assert!(matches!(stage, SocksReplyStage::Command));
                    assert_eq!(bytes.as_ref(), &[5, 0, 0, 255]);
                    assert!(!complete);
                }
            }
            other => panic!("lost the consumed reply: {other}"),
        }
    }
}
