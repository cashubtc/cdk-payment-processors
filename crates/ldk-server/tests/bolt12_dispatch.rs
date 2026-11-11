use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey};
use cdk_common::amount::Amount;
use cdk_common::common::FeeReserve;
use cdk_common::payment::{
    Bolt12OutgoingPaymentOptions, MintPayment, OutgoingPaymentOptions, PaymentIdentifier,
};
use cdk_common::{CurrencyUnit, MeltQuoteState, QuoteId};
use cdk_payment_processor_ldk_server::backend::{Config, LdkServerBackend};
use hyper::{service::service_fn, Body, Response};
use ldk_server_client::ldk_server_grpc::api::{
    Bolt12SendResponse, GetNodeInfoResponse, GetPaymentDetailsResponse,
};
use ldk_server_client::ldk_server_grpc::grpc::encode_grpc_frame;
use ldk_server_client::ldk_server_grpc::types::{
    payment_kind, Bolt12Offer, Payment, PaymentDirection, PaymentKind, PaymentStatus,
};
use lightning::offers::offer::OfferBuilder;
use prost::Message;
use tokio::task::{JoinHandle, JoinSet};
use tokio_rustls::{rustls, TlsAcceptor};

type Reply = (&'static str, Response<Body>);

struct MockLdk {
    backend: LdkServerBackend,
    replies: Arc<Mutex<VecDeque<Reply>>>,
    requests: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl MockLdk {
    async fn new(replies: Vec<Reply>) -> Self {
        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();
        let tls = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(
                vec![rustls::Certificate(cert.cert.der().to_vec())],
                rustls::PrivateKey(cert.key_pair.serialize_der()),
            )
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(tls));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend = LdkServerBackend::new(Config::new(
            listener.local_addr().unwrap().to_string(),
            "test-api-key".to_string(),
            cert.cert.pem().into_bytes(),
            FeeReserve {
                min_fee_reserve: 2.into(),
                percent_fee_reserve: 0.01,
            },
        ))
        .unwrap();
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let responses = replies.clone();
        let received = requests.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let acceptor = acceptor.clone();
                let responses = responses.clone();
                let received = received.clone();
                connections.spawn(async move {
                    let stream = acceptor.accept(stream).await.unwrap();
                    let service = service_fn(move |request: hyper::Request<Body>| {
                        let responses = responses.clone();
                        let received = received.clone();
                        async move {
                            let method =
                                request.uri().path().rsplit('/').next().unwrap().to_string();
                            received.lock().unwrap().push(method.clone());
                            let (expected, response) = responses
                                .lock()
                                .unwrap()
                                .pop_front()
                                .expect("unexpected RPC");
                            assert_eq!(method, expected);
                            Ok::<_, Infallible>(response)
                        }
                    });
                    let _ = hyper::server::conn::Http::new()
                        .serve_connection(stream, service)
                        .await;
                });
            }
        });
        Self {
            backend,
            replies,
            requests,
            task,
        }
    }

    fn assert_done(&self, expected_requests: &[&str]) {
        assert!(self.replies.lock().unwrap().is_empty());
        assert_eq!(*self.requests.lock().unwrap(), expected_requests);
    }
}

impl Drop for MockLdk {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn success(message: impl Message) -> Response<Body> {
    Response::builder()
        .header("content-type", "application/grpc+proto")
        .body(Body::from(
            encode_grpc_frame(&message.encode_to_vec()).to_vec(),
        ))
        .unwrap()
}

fn failure(status: u8) -> Response<Body> {
    Response::builder()
        .header("content-type", "application/grpc+proto")
        .header("grpc-status", status.to_string())
        .body(Body::empty())
        .unwrap()
}

fn options(expired: bool) -> OutgoingPaymentOptions {
    let key = SecretKey::from_slice(&[42; 32]).unwrap();
    let mut offer =
        OfferBuilder::new(PublicKey::from_secret_key(&Secp256k1::new(), &key)).amount_msats(20_000);
    if expired {
        offer = offer.absolute_expiry(Duration::from_secs(1));
    }
    OutgoingPaymentOptions::Bolt12(Box::new(Bolt12OutgoingPaymentOptions {
        offer: offer.build().unwrap(),
        max_fee_amount: None,
        timeout_secs: Some(0),
        melt_options: None,
        quote_id: QuoteId::new(),
    }))
}

async fn assert_failed(mock: &MockLdk, options: OutgoingPaymentOptions) {
    let quote = mock
        .backend
        .get_payment_quote(&CurrencyUnit::Sat, options.clone())
        .await
        .unwrap();
    assert_eq!(quote.state, MeltQuoteState::Unpaid);
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        mock.backend.make_payment(&CurrencyUnit::Sat, options),
    )
    .await
    .unwrap()
    .expect("a definite rejection must return an authoritative outcome");
    assert_eq!(response.status, MeltQuoteState::Failed);
    assert_eq!(response.total_spent, Amount::new(0, CurrencyUnit::Sat));
    assert!(response.payment_proof.is_none());
    // CDK 0.18.2 rechecks even an authoritative result. Unknown preserves the
    // initial Failed result without inventing an LDK payment that never existed.
    assert_eq!(
        mock.backend
            .check_outgoing_payment(&response.payment_lookup_id)
            .await
            .unwrap()
            .status,
        MeltQuoteState::Unknown
    );
}

#[tokio::test]
async fn offline_node_fails_before_dispatch() {
    let mut mock = MockLdk::new(vec![]).await;
    mock.task.abort();
    let _ = (&mut mock.task).await;
    assert_failed(&mock, options(false)).await;
    mock.assert_done(&[]);
}

#[tokio::test]
async fn expired_offer_fails_without_contacting_node() {
    let mock = MockLdk::new(vec![]).await;
    assert_failed(&mock, options(true)).await;
    mock.assert_done(&[]);
}

#[tokio::test]
async fn failed_preflight_does_not_send_a_payment() {
    let mock = MockLdk::new(vec![("GetNodeInfo", failure(14))]).await;
    assert_failed(&mock, options(false)).await;
    mock.assert_done(&["GetNodeInfo"]);
}

#[tokio::test]
async fn validation_and_auth_rejections_are_definite_failures() {
    for status in [3, 16] {
        let mock = MockLdk::new(vec![
            ("GetNodeInfo", success(GetNodeInfoResponse::default())),
            ("Bolt12Send", failure(status)),
        ])
        .await;
        assert_failed(&mock, options(false)).await;
        mock.assert_done(&["GetNodeInfo", "Bolt12Send"]);
    }
}

#[tokio::test]
async fn ambiguous_send_errors_are_not_reported_as_failed() {
    for response in [
        failure(9),
        failure(13),
        failure(14),
        Response::new(Body::empty()),
    ] {
        let mock = MockLdk::new(vec![
            ("GetNodeInfo", success(GetNodeInfoResponse::default())),
            ("Bolt12Send", response),
        ])
        .await;
        assert!(mock
            .backend
            .make_payment(&CurrencyUnit::Sat, options(false))
            .await
            .is_err());
        mock.assert_done(&["GetNodeInfo", "Bolt12Send"]);
    }
}

#[tokio::test]
async fn accepted_payment_keeps_its_id_when_status_is_unavailable() {
    for missing_payment in [false, true] {
        let unavailable = || {
            if missing_payment {
                success(GetPaymentDetailsResponse { payment: None })
            } else {
                failure(14)
            }
        };
        let mock = MockLdk::new(vec![
            ("GetNodeInfo", success(GetNodeInfoResponse::default())),
            (
                "Bolt12Send",
                success(Bolt12SendResponse {
                    payment_id: "02".repeat(32),
                }),
            ),
            ("GetPaymentDetails", unavailable()),
            ("GetPaymentDetails", unavailable()),
            (
                "GetPaymentDetails",
                success(GetPaymentDetailsResponse {
                    payment: Some(Payment {
                        id: "02".repeat(32),
                        kind: Some(PaymentKind {
                            kind: Some(payment_kind::Kind::Bolt12Offer(Bolt12Offer {
                                preimage: Some("03".repeat(32)),
                                ..Default::default()
                            })),
                        }),
                        amount_msat: Some(20_000),
                        fee_paid_msat: Some(1_000),
                        direction: PaymentDirection::Outbound as i32,
                        status: PaymentStatus::Succeeded as i32,
                        ..Default::default()
                    }),
                }),
            ),
        ])
        .await;
        let response = mock
            .backend
            .make_payment(&CurrencyUnit::Sat, options(false))
            .await
            .unwrap();
        assert_eq!(response.status, MeltQuoteState::Pending);
        assert_eq!(
            response.payment_lookup_id,
            PaymentIdentifier::PaymentId([2; 32])
        );
        assert_eq!(response.total_spent, Amount::new(0, CurrencyUnit::Sat));
        assert!(response.payment_proof.is_none());
        let unknown = mock
            .backend
            .check_outgoing_payment(&response.payment_lookup_id)
            .await
            .unwrap();
        assert_eq!(unknown.status, MeltQuoteState::Unknown);
        assert_eq!(unknown.payment_lookup_id, response.payment_lookup_id);
        let paid = mock
            .backend
            .check_outgoing_payment(&response.payment_lookup_id)
            .await
            .unwrap();
        assert_eq!(paid.status, MeltQuoteState::Paid);
        assert_eq!(paid.total_spent, Amount::new(21_000, CurrencyUnit::Msat));
        assert_eq!(paid.payment_proof, Some("03".repeat(32)));
        mock.assert_done(&[
            "GetNodeInfo",
            "Bolt12Send",
            "GetPaymentDetails",
            "GetPaymentDetails",
            "GetPaymentDetails",
        ]);
    }
}
