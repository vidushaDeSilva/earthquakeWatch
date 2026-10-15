# Sprint 3 — dispatch, delivery and landing contracts

This supplements DATA_CONTRACTS_V1.md; source contracts remain unchanged.

## Envelope v1

JSON fields: envelope_version=1, contract_version=usgs-summary-v1,
stream_generation, environment=dev, topic, capture_id, collection_id, source_id,
source_revision, source_item_id, source_item_index (zero-based feature ordinal),
source_event_id nullable, source_version_id nullable, captured_at,
raw_feature_sha256, feature (the complete parsed feature, including invalid items).

- source_item_id=H([source-item-v1,capture_id,index]).
- raw_feature_sha256 hashes canonical feature JSON, not its original substring.
- Source semantic version formula is unchanged from Sprint2. Unknown source
  fields stay in feature even when they are outside semantic revision selection.
- Kafka key is UTF8 USGS feature id when valid, else UTF8 source_item_id.
- Source item identity is independent of dispatch generation and physical offset.
- Envelope itself omits dispatch attempt/generation so deliberate replay keeps
  exactly the same message bytes within the stream generation.

H means SHA256 of canonical JSON of the typed array. Integral floats normalize to
integers in the runner's canonical encoder; hashes never depend on concatenation.

## Dispatch receipts

Each chunk seals its first item index, item count, SHA256 list for message values,
and acknowledged partition/offset for every item. The complete dispatch manifest
contains ordered chunk paths, hashes and counts. Receipt existence does not prove
that Kafka still retains the message. Re-send an unreceipted chunk on failure;
deliberate replay uses a new dispatch_generation. A capture with zero items still
has a complete manifest with zero chunks.

## Raw landing JSONL and manifest

One JSON row per physical Kafka delivery: delivery_id, stream_generation, topic,
partition, offset, kafka_timestamp_ms, key_b64, value_b64, headers_b64,
message_sha256. Null raw bytes remain JSON null, empty bytes become empty base64.
The value hash is null for a Kafka null/tombstone, SHA256 otherwise. Base64 is not
encryption. Access is controlled by the Databricks workspace/Volume permissions.

Delivery ID=H([delivery-v1,stream_generation,topic,partition,offset]).
Stream ID=H([stream-v1,dev,topic,stream_generation]).
Batch ID=H([landing-v1,stream_id,partition,offset_first,offset_last,data_sha256]).

Manifest fields include manifest_version=1, stream_id, stream_generation, topic,
partition, batch_id, offset_first/last, next_offset=last+1, row_count,
data_bytes, data_sha256, records_relative_path, first_run_id, landed_at,
state=LANDED. Records are contiguous within each partition; holes stop commits.
Manifest path is immutable and written after exact records.jsonl readback.

The boundary includes no end-to-end transaction: data upload, manifest upload,
Kafka commit, and each Delta MERGE are independent operations. Ordered evidence
and recovery are what make partial progress reviewable and replayable.

## Deferred acknowledgement

Every sealed batch remains eligible for Sprint4 regardless of Kafka group state.
Sprint3 imports LANDED metadata; it creates no Bronze ack or source ACCOUNTED
state. Sprint4 must reconcile each manifest delivery and every source item before
advancing those states. Until then, never delete source, dispatch or landing files.
