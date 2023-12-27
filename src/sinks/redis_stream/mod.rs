//! `redis` sink.
//!
//! Writes data to [redis](https://redis.io/).
mod config;
mod request_builder;
mod service;
mod sink;

#[cfg(test)]
mod tests;

#[cfg(feature = "redis-integration-tests")]
#[cfg(test)]
mod integration_tests;

use bytes::Bytes;
use redis::{/*aio::ConnectionManager,*/ streams::StreamMaxlen, RedisError, /*RedisResult*/};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::sinks::prelude::*;

use super::util::EncodedLength;

#[derive(Debug, Snafu)]
pub(super) enum RedisSinkError {
    #[snafu(display("Creating Redis producer failed: {}", source))]
    RedisCreateFailed { source: RedisError },
    #[snafu(display("Invalid key template: {}", source))]
    KeyTemplate { source: TemplateParseError },
    #[snafu(display("Error sending query: {}", source))]
    SendError { source: RedisError },
    #[snafu(display("Error serializing query: {}", source))]
    Serde { source: serde_json::Error },
}

#[derive(Copy, Clone, Debug, Derivative, Deserialize, Serialize, Eq, PartialEq)]
#[derivative(Default)]
#[serde(rename_all = "lowercase")]
pub enum MaxLenType {
    #[derivative(Default)]
    Equals,
    Approx,
}

#[derive(Clone, Debug, Derivative, Deserialize, Serialize, Eq, PartialEq)]
pub struct MaxLenOption {
    #[serde(alias = "type")]
    maxlen_type: MaxLenType,
    threshold: usize,
}

#[derive(Clone, Debug, Derivative, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub struct StreamOption {
    field: String,
    maxlen: Option<MaxLenOption>,
}

#[derive(Clone, Copy, Debug, Derivative)]
#[derivative(Default)]
pub enum DataType {
    #[derivative(Default)]
    Stream {
        // field: Template,
        maxlen: Option<StreamMaxlen>,
    },
}

/// Wrapper for an `Event` that also stored the rendered key.
pub(super) struct RedisEvent {
    event: Event,
    key: String,
}

impl Finalizable for RedisEvent {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.event.take_finalizers()
    }
}

impl ByteSizeOf for RedisEvent {
    fn allocated_bytes(&self) -> usize {
        self.event.allocated_bytes()
    }
}

impl GetEventCountTags for RedisEvent {
    fn get_tags(&self) -> TaggedEventsSent {
        self.event.get_tags()
    }
}

impl EstimatedJsonEncodedSizeOf for RedisEvent {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        self.event.estimated_json_encoded_size_of()
    }
}

#[derive(Clone)]
pub(super) struct RedisRequest {
    request: Vec<RedisKvEntry>,
    finalizers: EventFinalizers,
    metadata: RequestMetadata,
}

impl Finalizable for RedisRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.finalizers)
    }
}

impl MetaDescriptive for RedisRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

#[derive(Debug, Clone)]
pub(super) struct RedisKvEntry {
    key: String,
    value: Bytes,
}

impl EncodedLength for RedisKvEntry {
    fn encoded_length(&self) -> usize {
        self.value.len()
    }
}
