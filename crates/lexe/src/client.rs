//! Small SDK boundary used to exercise payment recovery without a live node.

use async_trait::async_trait;
use lexe::types::command::{
    CreateInvoiceRequest, CreateInvoiceResponse, GetPaymentRequest, GetPaymentResponse,
    GetUpdatedPaymentsRequest, GetUpdatedPaymentsResponse, ListPaymentsResponse, NodeInfo,
    PayInvoiceRequest,
};
use lexe::types::payment::{Payment, PaymentCreatedIndex, PaymentFilter};
use lexe::wallet::LexeWallet;
use lexe_api_core::def::UserNodeRunApi;
use lexe_api_core::error::{NodeApiError, NodeErrorKind};

/// Only a submission error can prove that the node did not accept a payment.
#[derive(Debug)]
pub(crate) enum SubmissionError {
    Rejected(anyhow::Error),
    Ambiguous(anyhow::Error),
}

impl From<NodeApiError> for SubmissionError {
    fn from(error: NodeApiError) -> Self {
        // Explicitly allow only failures before dispatch or rejection by the
        // request/authentication layer. In particular, Command errors have no
        // structured finality information (and may describe a duplicate).
        // Never infer non-acceptance from a message or an HTTP status alone.
        match error.kind {
            NodeErrorKind::Building
            | NodeErrorKind::Connect
            | NodeErrorKind::Rejection
            | NodeErrorKind::ClientAuth
            | NodeErrorKind::InsufficientScope
            | NodeErrorKind::BadAuth => Self::Rejected(error.into()),
            _ => Self::Ambiguous(error.into()),
        }
    }
}

#[async_trait]
pub(crate) trait LexeClient: Send + Sync {
    async fn node_info(&self) -> anyhow::Result<NodeInfo>;
    async fn create_invoice(
        &self,
        req: CreateInvoiceRequest,
    ) -> anyhow::Result<CreateInvoiceResponse>;
    async fn submit_invoice(
        &self,
        req: PayInvoiceRequest,
    ) -> Result<PaymentCreatedIndex, SubmissionError>;
    async fn wait_for_payment(&self, index: PaymentCreatedIndex) -> anyhow::Result<Payment>;
    async fn sync_payments(&self) -> anyhow::Result<()>;
    fn list_payments(
        &self,
        limit: usize,
        after: Option<&PaymentCreatedIndex>,
    ) -> anyhow::Result<ListPaymentsResponse>;
    async fn get_payment(&self, req: GetPaymentRequest) -> anyhow::Result<GetPaymentResponse>;
    async fn get_updated_payments(
        &self,
        req: GetUpdatedPaymentsRequest,
    ) -> anyhow::Result<GetUpdatedPaymentsResponse>;
}

#[async_trait]
impl LexeClient for LexeWallet {
    async fn node_info(&self) -> anyhow::Result<NodeInfo> {
        self.node_info().await
    }

    async fn create_invoice(
        &self,
        req: CreateInvoiceRequest,
    ) -> anyhow::Result<CreateInvoiceResponse> {
        self.create_invoice(req).await
    }

    async fn submit_invoice(
        &self,
        req: PayInvoiceRequest,
    ) -> Result<PaymentCreatedIndex, SubmissionError> {
        let id = req.invoice.payment_id();
        let req = lexe_api_core::models::command::PayInvoiceRequest::try_from(req)
            .map_err(SubmissionError::Rejected)?;
        // LexeWallet::pay_invoice also waits for settlement. Classifying its
        // errors would confuse a polling/auth failure AFTER acceptance with
        // a rejected submission. This single-request API does not poll/retry.
        let response = self.node_client().pay_invoice(req).await?;
        Ok(PaymentCreatedIndex {
            created_at: response.created_at,
            id,
        })
    }

    async fn wait_for_payment(&self, index: PaymentCreatedIndex) -> anyhow::Result<Payment> {
        self.wait_for_payment(index, None).await
    }

    async fn sync_payments(&self) -> anyhow::Result<()> {
        self.sync_payments().await.map(|_| ())
    }

    fn list_payments(
        &self,
        limit: usize,
        after: Option<&PaymentCreatedIndex>,
    ) -> anyhow::Result<ListPaymentsResponse> {
        self.list_payments(&PaymentFilter::All, None, Some(limit), after)
    }

    async fn get_payment(&self, req: GetPaymentRequest) -> anyhow::Result<GetPaymentResponse> {
        self.get_payment(req).await
    }

    async fn get_updated_payments(
        &self,
        req: GetUpdatedPaymentsRequest,
    ) -> anyhow::Result<GetUpdatedPaymentsResponse> {
        self.get_updated_payments(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_definite_submission_rejections_are_terminal() {
        for kind in [
            NodeErrorKind::Building,
            NodeErrorKind::Connect,
            NodeErrorKind::Rejection,
            NodeErrorKind::ClientAuth,
            NodeErrorKind::InsufficientScope,
            NodeErrorKind::BadAuth,
        ] {
            let error = NodeApiError {
                kind,
                ..Default::default()
            };
            assert!(matches!(
                SubmissionError::from(error),
                SubmissionError::Rejected(_)
            ));
        }
        for kind in [
            NodeErrorKind::Unknown(999),
            NodeErrorKind::UnknownReqwest,
            NodeErrorKind::Timeout,
            NodeErrorKind::Decode,
            NodeErrorKind::Server,
            NodeErrorKind::AtCapacity,
            NodeErrorKind::General,
            NodeErrorKind::WrongUserPk,
            NodeErrorKind::WrongNodePk,
            NodeErrorKind::WrongMeasurement,
            NodeErrorKind::Provision,
            NodeErrorKind::Proxy,
            NodeErrorKind::Command,
            NodeErrorKind::NotFound,
        ] {
            let error = NodeApiError {
                kind,
                msg: "rejected: insufficient balance or duplicate payment".into(),
                ..Default::default()
            };
            assert!(matches!(
                SubmissionError::from(error),
                SubmissionError::Ambiguous(_)
            ));
        }
    }
}
