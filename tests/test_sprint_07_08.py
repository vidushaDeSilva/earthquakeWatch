"""Exercise capture recovery, frozen API retries, retention proofs and rollback.

These tests execute the actual engines with in-memory broker/Volume models.
They do not claim a Databricks deployment, real Delta commits or quota evidence.
"""

import copy
import runpy
import unittest
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from quake_pipeline.capture import Collector
from quake_pipeline.config import Settings
from quake_pipeline.contracts import SOURCES, decode_json, digest, json_bytes, stable_id, utc_text
from quake_pipeline.errors import BudgetExceeded, ContentConflict, ContractError
from quake_pipeline.http import Response
from quake_pipeline.jobs import sync_ledger, wait_ledger
from quake_pipeline.outbox import InjectedFailure, bind_stream, stream_id
from quake_pipeline.landing import land, recover
from quake_pipeline.reconciliation import (CatalogCollector, checked_source, gap_plan,
    inventory, recover_captures, repair_gap)
from test_sprint_03 import MemoryKafka, MemoryVolume, feature, settings
from test_sprint_04 import NOW
from test_sprint_05_06 import ROOT, LakeStore, event, s4, s5, s6

G7 = runpy.run_path(str(ROOT / 'databricks/sprint_07_reconciliation_library.py'), init_globals=vars(s6))
s7 = SimpleNamespace(**G7)
s8 = SimpleNamespace(**runpy.run_path(str(ROOT / 'databricks/sprint_08_lifecycle_library.py'), init_globals=G7))
CLOUD = Settings(host='https://example.cloud.databricks.com', token='fixture')


class QueueHTTP:
    """Return controlled external responses without accessing a network."""

    def __init__(self, replies):
        """Preserve a sequence of statuses, payloads and injected transport failures."""
        self.replies, self.calls = list(replies), []

    def request(self, service, method, url, **kwargs):
        """Record exact API requests and return one bounded fixture response."""
        self.calls.append((service, method, url, kwargs))
        if not self.replies:
            raise AssertionError('Unexpected HTTP request')
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


class JobWaitTests(unittest.TestCase):
    """Keep polling a run across runner waits without another run-now request."""

    def test_attached_wait_does_not_launch_again(self):
        """A queued run may later succeed while GitHub only makes GET requests."""
        client = QueueHTTP([Response(200, json_bytes({'state': {'life_cycle_state': state,
            'result_state': result}})) for state, result in
            [('QUEUED', None), ('RUNNING', None), ('TERMINATED', 'SUCCESS')]])
        self.assertEqual(wait_ledger(CLOUD, '123', client, sleeper=lambda _: None), '123')
        self.assertEqual([call[1] for call in client.calls], ['GET', 'GET', 'GET'])

    def test_wait_budget_preserves_running_run(self):
        """Expiry reports the run ID and never cancels or relaunches Databricks."""
        client = QueueHTTP([Response(200, json_bytes({'state': {'life_cycle_state': 'RUNNING'}}))])
        ticks = iter([0, 0, 60])
        with self.assertRaisesRegex(BudgetExceeded, '123'):
            wait_ledger(CLOUD, '123', client, sleeper=lambda _: None,
                        monotonic=lambda: next(ticks), wait_seconds=60)
        self.assertEqual(len(client.calls), 1)

    def test_nonterminal_success_field_does_not_end_wait(self):
        """Only a terminal SUCCESS can certify a completed multi-task Job."""
        client = QueueHTTP([Response(200, json_bytes({'state': {'life_cycle_state':'RUNNING',
            'result_state':'SUCCESS'}})), Response(200, json_bytes({'state': {
            'life_cycle_state':'TERMINATED','result_state':'SUCCESS'}}))])
        self.assertEqual(wait_ledger(CLOUD, '123', client, sleeper=lambda _: None), '123')
        self.assertEqual(len(client.calls), 2)


def feed(features):
    """Encode a generated USGS collection with matching metadata counts."""
    return json_bytes({'type':'FeatureCollection', 'metadata':{'count':len(features),
        'status':200, 'generated':int(NOW.timestamp()*1000)}, 'features':features})


def frozen(volume, features=None, hour=0, source=None):
    """Freeze producer-compatible source evidence with the actual Sprint 2 collector."""
    source = source or SOURCES['hourly'][0]
    http = QueueHTTP([Response(200, feed(features or []))])
    collector = Collector(CLOUD, http, volume, now=lambda:NOW)
    return collector.collect(source, 'hourly', NOW-timedelta(hours=hour+1),
        NOW-timedelta(hours=hour), stable_id('run',hour), True)[0]


class RecoveryTests(unittest.TestCase):
    """Verify real capture-ID discovery and resend behavior independently of collection windows."""

    def setUp(self):
        """Keep Kafka and source bytes in isolated in-memory fixtures."""
        self.volume, self.kafka, self.stream = MemoryVolume(), MemoryKafka(), settings()
        bind_stream(self.volume, self.kafka, self.stream)

    def test_two_older_captured_responses_are_dispatched(self):
        """Recover both saved captures without fetching a newer live feed."""
        first = frozen(self.volume, [feature(0)], hour=1)
        second = frozen(self.volume, [feature(1)], hour=0)
        result = recover_captures(CLOUD, QueueHTTP([]), self.volume, self.kafka, self.stream, lambda e:None)
        self.assertEqual(result['selected_captures'],2)
        self.assertEqual({decode_json(r.value)['capture_id'] for r in self.kafka.records[0]},
                         {first['capture_id'],second['capture_id']})

    def test_retry_does_not_reproduce_acked_chunk(self):
        """A repeated dispatch reuses immutable chunk acknowledgement receipts."""
        frozen(self.volume,[feature()])
        for _ in range(2):
            recover_captures(CLOUD,QueueHTTP([]),self.volume,self.kafka,self.stream,lambda e:None)
        self.assertEqual(len(self.kafka.records[0]),1)

    def test_lost_produce_ack_can_duplicate_delivery_with_same_item(self):
        """Unreceipted sends may repeat while their stable source ordinal remains identical."""
        frozen(self.volume,[feature()])
        with self.assertRaises(InjectedFailure):
            recover_captures(CLOUD,QueueHTTP([]),self.volume,self.kafka,self.stream,lambda e:None,fail_at='after-produce')
        recover_captures(CLOUD,QueueHTTP([]),self.volume,self.kafka,self.stream,lambda e:None)
        ids=[decode_json(r.value)['source_item_id'] for r in self.kafka.records[0]]
        self.assertEqual(len(ids),2)
        self.assertEqual(len(set(ids)),1)

    def test_accounted_capture_skips_expired_response(self):
        """A valid retained accounting receipt prevents trying to dispatch expired bytes."""
        m=frozen(self.volume,[feature()])
        self.volume.files['receipts/captures/'+m['capture_id']+'.json']=json_bytes({
            'capture_id':m['capture_id'],'response_sha256':m['response_sha256'],'state':'ACCOUNTED',
            'expected_item_count':1,'accounted_item_count':1})
        del self.volume.files[m['response_relative_path']]
        self.assertEqual(inventory(self.volume),[])

    def test_bad_accounting_receipt_blocks_recovery(self):
        """A mismatched completion cannot silently hide an unprocessed capture."""
        m=frozen(self.volume,[feature()])
        self.volume.files['receipts/captures/'+m['capture_id']+'.json']=b'{}'
        with self.assertRaises(ContractError): inventory(self.volume)

    def test_missing_selection_is_recovered(self):
        """An interrupted final selection write reuses the already sealed response."""
        m=frozen(self.volume,[feature()])
        del self.volume.files['collections/'+m['collection_id']+'/selected.json']
        self.assertEqual(inventory(self.volume)[0]['capture_id'],m['capture_id'])

    def test_corrupt_source_prevents_any_dispatch(self):
        """Source byte validation occurs before messages are sent."""
        m=frozen(self.volume,[feature()])
        self.volume.files[m['response_relative_path']]=b'changed'
        with self.assertRaises(ContractError):
            recover_captures(CLOUD,QueueHTTP([]),self.volume,self.kafka,self.stream,lambda e:None)
        self.assertEqual(self.kafka.records[0],[])

    def test_capture_limit_makes_bounded_progress(self):
        """Report remaining work rather than silently claiming the full backlog was drained."""
        frozen(self.volume,[feature(0)],1)
        frozen(self.volume,[feature(1)],0)
        result=recover_captures(CLOUD,QueueHTTP([]),self.volume,self.kafka,self.stream,lambda e:None,maximum=1)
        self.assertEqual(result['remaining_captures'],1)

    def test_unknown_source_is_not_dispatched(self):
        """A foreign URL in a capture manifest does not turn the runner into an arbitrary HTTP client."""
        with self.assertRaises(ContractError): checked_source({'source_id':'usgs-event-check','source_url':'https://evil.test/query'})

    def test_inventory_bound_fails_visibly(self):
        """A bounded inventory cannot silently omit additional captures."""
        frozen(self.volume,[feature()])
        with self.assertRaises(BudgetExceeded): inventory(self.volume,maximum=0)

    def test_expired_offsets_require_explicit_reviewed_plan(self):
        """Normal recovery leaves an expired broker checkpoint unchanged."""
        frozen(self.volume,[feature()])
        self.kafka.starts[0]=10
        with self.assertRaises(ContractError): land(self.volume,self.kafka,self.stream,'run',lambda e:None)
        self.assertIsNone(self.kafka.commits[0])

    def test_reviewed_gap_resets_and_replays_capture(self):
        """The explicit gap action preserves source evidence and records its limited recovery scope."""
        m=frozen(self.volume,[feature()])
        self.kafka.starts[0]=10
        gid=gap_plan(self.volume,self.kafka,self.stream)
        repair_gap(CLOUD,QueueHTTP([]),self.volume,self.kafka,self.stream,gid,'run',lambda e:None)
        self.assertEqual(self.kafka.commits[0],11)
        receipt=decode_json(self.volume.files['reconciliation/gaps/'+gid+'/ready.json'])
        self.assertEqual(receipt['scope'],'SELECTED_CAPTURE_EVIDENCE_ONLY')
        self.assertEqual(decode_json(self.kafka.records[0][0].value)['capture_id'],m['capture_id'])

    def test_reviewed_gap_rejects_new_retention_boundary(self):
        """A broker range that moved after review needs a new plan."""
        frozen(self.volume,[feature()])
        self.kafka.starts[0]=10
        gid=gap_plan(self.volume,self.kafka,self.stream)
        self.kafka.starts[0]=11
        with self.assertRaises(ContractError): repair_gap(CLOUD,QueueHTTP([]),self.volume,self.kafka,self.stream,gid,'r',lambda e:None)

    def test_gap_retry_is_idempotent_after_landing(self):
        """An already completed reviewed reset never produces its sources twice."""
        frozen(self.volume,[feature()])
        self.kafka.starts[0]=10
        gid=gap_plan(self.volume,self.kafka,self.stream)
        for _ in range(2): repair_gap(CLOUD,QueueHTTP([]),self.volume,self.kafka,self.stream,gid,'r',lambda e:None)
        self.assertEqual(len(self.kafka.records[0]),1)


class CatalogTests(unittest.TestCase):
    """Verify exact source evidence, empty queries, deleted events and bounded backfill."""

    def collector(self,replies,volume=None):
        """Create a paced collector with deterministic clocks and no real waits."""
        http=QueueHTTP(replies)
        volume=volume or MemoryVolume()
        return CatalogCollector(CLOUD,http,volume,'epoch1',now=lambda:NOW,sleeper=lambda s:None),http,volume

    def test_event_detail_is_wrapped_and_original_preserved(self):
        """A detail Feature becomes one source item without changing its original bytes."""
        raw=json_bytes(event())
        c,http,v=self.collector([Response(200,raw)])
        r=c.collect({'eventid':'eq0'},'EVENT_CHECK','r',NOW,NOW)
        self.assertEqual(v.files[r['original_relative_path']],raw)
        self.assertEqual(r['generated_basis'],'RESPONSE_RECEIVED_NOT_SOURCE_GENERATION')
        self.assertEqual(inventory(v)[0]['capture_id'],r['capture_id'])

    def test_retry_does_not_call_live_endpoint(self):
        """The same epoch reuses the sealed request and frozen response."""
        c,http,v=self.collector([Response(200,json_bytes(event()))])
        one=c.collect({'eventid':'eq0'},'EVENT_CHECK','r',NOW,NOW)
        two=c.collect({'eventid':'eq0'},'EVENT_CHECK','r2',NOW,NOW)
        self.assertEqual(one,two)
        self.assertEqual(len(http.calls),1)

    def test_single_event_404_does_not_invent_deletion(self):
        """Missing event lookup remains unresolved, with no source tombstone."""
        c,http,v=self.collector([Response(404,b'not found')])
        r=c.collect({'eventid':'eq0'},'EVENT_CHECK','r',NOW,NOW)
        self.assertEqual(r['outcome'],'UNRESOLVED')
        self.assertIsNone(r['capture_id'])
        self.assertEqual(inventory(v),[])

    def test_explicit_deleted_event_is_frozen(self):
        """A source status=deleted survives into the normal Bronze/Silver path."""
        c,http,v=self.collector([Response(200,json_bytes(event(status='deleted')))])
        r=c.collect({'eventid':'eq0'},'EVENT_CHECK','r',NOW,NOW)
        self.assertIsNotNone(r['capture_id'])
        self.assertIn('includedeleted=true',http.calls[0][2])

    def test_explicit_alias_is_accepted(self):
        """A preferred-ID change is accepted only with a source-provided alias link."""
        c,_,_=self.collector([Response(200,json_bytes(event(ids=',old,eq0,')))])
        self.assertEqual(c.collect({'eventid':'old'},'EVENT_CHECK','r',NOW,NOW)['outcome'],'CAPTURED')

    def test_unlinked_event_id_is_rejected(self):
        """A different event without an alias cannot replace the requested event."""
        c,_,_=self.collector([Response(200,json_bytes(event()))])
        with self.assertRaises(ContractError): c.collect({'eventid':'other'},'EVENT_CHECK','r',NOW,NOW)

    def test_empty_catalog_slice_is_valid(self):
        """Two agreeing zero counts and HTTP 204 produce an explicit empty capture."""
        c,http,v=self.collector([Response(200,b'0'),Response(204,b''),Response(200,b'0')])
        result=c.backfill(NOW.replace(hour=0)-timedelta(days=1),NOW.replace(hour=0),'r')
        self.assertEqual(result[0]['count_before'],0)
        self.assertEqual(inventory(v)[0]['expected_item_count'],0)

    def test_count_change_is_not_silently_accepted(self):
        """A truncated or changing catalog slice cannot be marked complete."""
        c,_,_=self.collector([Response(200,b'1'),Response(200,feed([event()])),Response(200,b'2')])
        with self.assertRaises(ContractError): c.backfill(NOW.replace(hour=0)-timedelta(days=1),NOW.replace(hour=0),'r')

    def test_frozen_http_checkpoint_survives_later_response_change(self):
        """After a count-check interruption, retry keeps the original earthquake response."""
        c,http,v=self.collector([Response(200,b'1'),Response(200,feed([event()])),RuntimeError('interrupted'),Response(200,b'1')])
        start,end=NOW.replace(hour=0)-timedelta(days=1),NOW.replace(hour=0)
        with self.assertRaises(RuntimeError): c.backfill(start,end,'r')
        result=c.backfill(start,end,'r')
        self.assertEqual(result[0]['outcome'],'CAPTURED')
        self.assertEqual(sum('/query?' in call[2] for call in http.calls),1)

    def test_dense_slice_is_split_without_offset_paging(self):
        """A capped day is subdivided and each smaller query has independent count evidence."""
        c,http,v=self.collector([Response(200,b'10000'),Response(200,b'0'),Response(204,b''),Response(200,b'0'),
            Response(200,b'0'),Response(204,b''),Response(200,b'0')])
        results=c.backfill(NOW.replace(hour=0)-timedelta(days=1),NOW.replace(hour=0),'r')
        self.assertEqual(len(results),2)
        self.assertTrue(all('offset=' not in call[2] for call in http.calls))

    def test_backfill_is_half_open(self):
        """The inclusive USGS end is reduced one millisecond to match project windows."""
        c,http,_=self.collector([Response(200,b'0'),Response(204,b''),Response(200,b'0')])
        c.backfill(NOW.replace(hour=0)-timedelta(days=1),NOW.replace(hour=0),'r')
        self.assertTrue(any('23%3A59%3A59.999000Z' in call[2] for call in http.calls))

    def test_backfill_rejects_partial_days_and_large_ranges(self):
        """Only bounded complete UTC days can claim a historical daily correction."""
        for start,end in ((NOW-timedelta(days=1),NOW),(NOW.replace(hour=0)-timedelta(days=8),NOW.replace(hour=0))):
            c,_,_=self.collector([])
            with self.assertRaises(ContractError): c.backfill(start,end,'r')

    def test_original_response_corruption_is_detected_on_retry(self):
        """A request receipt does not hide altered original bytes."""
        c,_,v=self.collector([Response(200,json_bytes(event()))])
        r=c.collect({'eventid':'eq0'},'EVENT_CHECK','r',NOW,NOW)
        v.files[r['original_relative_path']]=b'changed'
        with self.assertRaises(ContractError): c.collect({'eventid':'eq0'},'EVENT_CHECK','r',NOW,NOW)


class LifecycleMemory(LakeStore):
    """Run the production lifecycle engine with modeled independent cloud mutations."""

    def __init__(self):
        """Bind exact raw action candidates and audit dictionaries."""
        super().__init__()
        self.actions,self.records,self.rollbacks,self.blocked=[],{}, {},False
        self.utils=SimpleNamespace(fs=SimpleNamespace(rm=self.remove))

    def guard_maintenance(self):
        """Model unresolved inputs or holds blocking deletion."""
        if self.blocked: raise s7.RecoveryError('Data hold')

    def path(self,value):
        """Keep fixture paths independent of the host filesystem."""
        return value

    def optional_limit(self,path,limit=16777216):
        """Return bounded fixture bytes through the same optional-source interface."""
        return self.files.get(path)

    def remove(self,path,recursive=False):
        """Delete one modeled cloud file while prohibiting recursive removal."""
        if recursive: raise AssertionError('Recursive delete is prohibited')
        self.files.pop(path,None)

    def retention_candidates(self):
        """Return exact inspectable actions that are still eligible."""
        return [a for a in self.actions if a['path'] in self.files], {'protected_publications':0}

    def save_plan(self,pid,content):
        """Keep the plan ledger without changing its immutable manifest."""
        self.records[pid]=copy.deepcopy(content)

    def start_action(self,pid,action):
        """Persist mutation intent before the production delete implementation runs."""
        self.records.setdefault(action['action_id'],{'status':'RUNNING','plan_id':pid})

    def finish_action(self,pid,action):
        """Record completion only after postcondition checks."""
        self.records[action['action_id']]['status']='SUCCESS'

    def perform_action(self,action,fail_at):
        """Exercise production raw-byte checks, retirement ordering and readback."""
        return s8.SparkLifecycleStore.perform_action(self,action,fail_at)

    def verify_release(self,publication):
        """Use real output-digest validation against modeled release rows."""
        for name,keys in s6.GOLD_KEYS.items():
            rows=[r for r in self.rows(name) if r['publication_id']==publication['publication_id']]
            if s5.s56_digest(rows,keys)!=publication['output_hashes'][name]:
                raise s7.RecoveryError('Invalid release digest')

    def rollback_record(self,aid):
        """Read a prepared or completed rollback audit."""
        return copy.deepcopy(self.rollbacks.get(aid))

    def save_rollback(self,row):
        """Persist rollback intent before pointer mutation."""
        self.rollbacks[row['action_id']]=copy.deepcopy(row)

    def rollback_pointer(self,row,run_id):
        """Model a single pointer transaction containing both target and exclusion list."""
        if self.point['pointer_generation']!=row['expected_generation']:
            raise s7.RecoveryError('CAS changed')
        self.point.update(publication_id=row['target_publication_id'],previous_publication_id=row['from_publication_id'],
            pointer_generation=row['expected_generation']+1,run_id=run_id,updated_at=self.now(),
            excluded_publication_ids=[row['from_publication_id']])

    def complete_rollback(self,row):
        """Repair a missing final audit after an already successful pointer switch."""
        row.update(state='APPLIED',finished_at=self.now())
        self.save_rollback(row)


def file_action(store,related=False):
    """Prepare a real raw-retirement action with cryptographic byte expectations."""
    store.files['raw.json']=b'original'
    action={'kind':'CAPTURE_FILE','scope_id':stable_id('capture'),'path':'raw.json',
        'sha256':digest(b'original'),'bytes':8,'retirement_path':'retired.json',
        'retirement':{'state':'RETIRED','capture_id':stable_id('capture')},'related_files':[]}
    if related:
        store.files['original.json']=b'original'
        action['related_files']=[{'path':'original.json','sha256':digest(b'original'),'bytes':8}]
    action['action_id']=stable_id('retention-action-v1',action)
    store.actions=[action]
    return action


class LifecycleTests(unittest.TestCase):
    """Test planned deletions, protected snapshots and conditional rollback recovery."""

    def test_retention_plan_does_not_delete_data(self):
        """A review plan records exact actions while preserving all bytes."""
        store=LifecycleMemory();file_action(store)
        result=s8.LifecycleEngine(store,'r').plan()
        self.assertIn('raw.json',store.files)
        self.assertEqual(result['action_count'],1)

    def test_apply_retires_before_removing_bytes(self):
        """Retained proof survives successful deletion and an identical retry."""
        store=LifecycleMemory();file_action(store)
        plan=s8.LifecycleEngine(store,'r').plan()
        for _ in range(2): s8.LifecycleEngine(store,'r').apply(plan['plan_id'])
        self.assertNotIn('raw.json',store.files)
        self.assertEqual(decode_json(store.files['retired.json'])['state'],'RETIRED')

    def test_interrupted_retirement_is_recoverable(self):
        """A failure after retirement proof leaves bytes safe until a clean retry."""
        store=LifecycleMemory();file_action(store)
        plan=s8.LifecycleEngine(store,'r').plan()
        with self.assertRaises(s4.BronzeInjectedFailure): s8.LifecycleEngine(store,'r','after-retirement').apply(plan['plan_id'])
        self.assertIn('raw.json',store.files)
        s8.LifecycleEngine(store,'r').apply(plan['plan_id'])
        self.assertNotIn('raw.json',store.files)

    def test_interrupted_delete_resumes_audit(self):
        """A lost delete reply is resolved by absent bytes plus retained proof."""
        store=LifecycleMemory();action=file_action(store)
        plan=s8.LifecycleEngine(store,'r').plan()
        with self.assertRaises(s4.BronzeInjectedFailure): s8.LifecycleEngine(store,'r','after-delete').apply(plan['plan_id'])
        s8.LifecycleEngine(store,'r').apply(plan['plan_id'])
        self.assertEqual(store.records[action['action_id']]['status'],'SUCCESS')

    def test_related_original_bytes_are_also_removed(self):
        """Raw source retention covers exact HTTP bytes as well as the Kafka wrapper."""
        store=LifecycleMemory();file_action(store,related=True)
        plan=s8.LifecycleEngine(store,'r').plan()
        s8.LifecycleEngine(store,'r').apply(plan['plan_id'])
        self.assertNotIn('original.json',store.files)

    def test_changed_planned_bytes_block_apply(self):
        """A reviewed action cannot delete replacement content at the same path."""
        store=LifecycleMemory();file_action(store)
        plan=s8.LifecycleEngine(store,'r').plan();store.files['raw.json']=b'changed'
        with self.assertRaises(s7.RecoveryError): s8.LifecycleEngine(store,'r').apply(plan['plan_id'])

    def test_active_hold_blocks_cleanup(self):
        """An unresolved recovery hold protects data even after planning."""
        store=LifecycleMemory();file_action(store)
        plan=s8.LifecycleEngine(store,'r').plan();store.blocked=True
        with self.assertRaises(s7.RecoveryError): s8.LifecycleEngine(store,'r').apply(plan['plan_id'])
        self.assertIn('raw.json',store.files)

    def test_expired_plan_requires_new_review(self):
        """Old deletion decisions cannot be applied indefinitely."""
        store=LifecycleMemory();file_action(store)
        plan=s8.LifecycleEngine(store,'r').plan();store.clock+=timedelta(days=2)
        with self.assertRaises(s7.RecoveryError): s8.LifecycleEngine(store,'r').apply(plan['plan_id'])

    def test_open_quarantine_and_current_versions_are_retained(self):
        """Row retention preserves unresolved evidence and current ordering state."""
        old=NOW-timedelta(days=400)
        tables={name:[] for name in s8.LIFECYCLE_TABLE_KEYS}
        tables['silver.events_current']=[{'source_version_id':'current','silver_version_id':'current'}]
        tables['bronze.event_versions']=[{'source_version_id':'current','first_ingested_at':old},
            {'source_version_id':'expired','first_ingested_at':old}]
        tables['silver.event_versions']=[{'silver_version_id':'current','normalized_at':old},
            {'silver_version_id':'expired','normalized_at':old}]
        tables['bronze.quarantine']=[{'resolution_status':'OPEN','resolved_at':None}]
        selected=s8.retention_row_candidates(tables,NOW,set())
        self.assertEqual([r['source_version_id'] for r in selected['bronze.event_versions']],['expired'])
        self.assertEqual([r['silver_version_id'] for r in selected['silver.event_versions']],['expired'])
        self.assertEqual(selected['bronze.quarantine'],[])

    def test_serving_and_previous_release_are_never_expired(self):
        """An old active/previous release remains protected beyond ordinary age limits."""
        pointer={'publication_id':'active','previous_publication_id':'previous'}
        self.assertEqual(s8.retention_publications([],pointer,[],NOW),{'active','previous'})

    def test_latest_daily_revision_protects_its_publication(self):
        """Daily history cannot silently lose the output release it references."""
        pub={'publication_id':'daily','state':'READY','is_daily_snapshot':False,
             'created_at':NOW-timedelta(days=10),'as_of_utc':NOW-timedelta(days=10)}
        day={'publication_id':'daily','event_date':NOW.date()-timedelta(days=10),'as_of_utc':pub['as_of_utc']}
        keep=s8.retention_publications([pub],{'publication_id':None,'previous_publication_id':None},[day],NOW)
        self.assertIn('daily',keep)

    def publications(self):
        """Create two complete real Gold releases in the memory lake."""
        store=LifecycleMemory()
        batch,_=store.add_bronze([event()]);s5.SilverEngine(store,'silver').process([batch]);store.ready_sources()
        first=s6.GoldEngine(store,'first').process(action='publish')['publication_id']
        store.clock+=timedelta(minutes=1)
        second=s6.GoldEngine(store,'second').process(action='publish')['publication_id']
        return store,first,second

    def test_rollback_switches_whole_release(self):
        """One pointer switch makes the retained prior facts current and excludes the bad release."""
        store,first,second=self.publications()
        result=s8.LifecycleEngine(store,'rollback').rollback(first,2,'incorrect policy')
        self.assertEqual(result['status'],'APPLIED')
        self.assertEqual(store.point['publication_id'],first)
        self.assertIn(second,store.point['excluded_publication_ids'])

    def test_rollback_recovers_after_pointer_fault(self):
        """A retry repairs its audit without incrementing the pointer twice."""
        store,first,_=self.publications()
        with self.assertRaises(s4.BronzeInjectedFailure):
            s8.LifecycleEngine(store,'rollback','after-pointer').rollback(first,2,'incorrect policy')
        result=s8.LifecycleEngine(store,'retry').rollback(first,2,'incorrect policy')
        self.assertEqual(result['status'],'ALREADY_APPLIED')
        self.assertEqual(store.point['pointer_generation'],3)

    def test_rollback_generation_change_is_rejected(self):
        """A reviewed rollback cannot overwrite a release activated afterward."""
        store,first,_=self.publications()
        with self.assertRaises(s7.RecoveryError): s8.LifecycleEngine(store,'r').rollback(first,1,'incorrect policy')

    def test_rollback_requires_complete_retained_output(self):
        """An old READY flag alone does not authorize a damaged snapshot."""
        store,first,_=self.publications()
        store.tables['gold.window_snapshots']={k:r for k,r in store.tables['gold.window_snapshots'].items() if r['publication_id']!=first}
        with self.assertRaises(s7.RecoveryError): s8.LifecycleEngine(store,'r').rollback(first,2,'damaged output')

    def test_historical_backfill_daily_row_uses_pinned_current(self):
        """A verified old scope can revise a historical day outside the watchlist horizon."""
        feature_value=event(seconds_ago=40*86400)
        row=s5.silver_normalize(feature_value,stable_id('event'),s4.bronze_version(feature_value),NOW)
        row.update(first_observed_at=NOW,last_confirmed_at=NOW,has_unresolved_conflict=False)
        day=row['event_time'].date().isoformat()
        pub={'publication_id':stable_id('pub'),'as_of_utc':NOW,'coverage_start':NOW-timedelta(days=30),
             'policy_version':'impact-v1','blocking_issues':[],'coverage_status':'FEED_SNAPSHOTS_ONLY',
             'source_checks_json':json_bytes({'historical_dates':[day]}).decode()}
        outputs=s6.gold_outputs([row],pub,{'magnitude':6,'mmi':6,'significance':600})
        historic=[r for r in outputs['gold.daily_history'] if r['event_date'].isoformat()==day]
        self.assertEqual(historic[0]['event_count'],1)
        self.assertEqual(outputs['gold.watchlist_snapshots'],[])


class MetadataFrame:
    """Model only metadata count filtering needed by the production reconciler."""

    def __init__(self, rows):
        """Keep small copied metadata rows without emulating Spark transformations."""
        self.data=list(rows)

    def where(self, expression):
        """Filter the one production quarantine predicate exercised by these tests."""
        if expression!="resolution_status='OPEN'": raise AssertionError(expression)
        return MetadataFrame(r for r in self.data if r['resolution_status']=='OPEN')

    def limit(self, value):
        """Bound a metadata count check."""
        return MetadataFrame(self.data[:value])

    def count(self):
        """Return a deterministic metadata row count."""
        return len(self.data)


class ReconciliationLake(LakeStore):
    """Exercise the real source-report and reconciliation methods over actual layer engines."""

    def __init__(self):
        """Add recovery tables while retaining all producer-compatible lake fixtures."""
        super().__init__()
        self.root=s4.BRONZE_ROOT
        for name in s7.S78_SCHEMAS: self.tables[name]={}

    def names(self, relative=''):
        """List fixture Volume entries without local filesystem access."""
        prefix=relative+'/' if relative else ''
        result={}
        for path in self.files:
            if path.startswith(prefix):
                suffix=path[len(prefix):]
                result[suffix.split('/')[0]]='/' in suffix
        return result

    def table(self,name):
        """Expose the exact small quarantine metadata check to the real reconciler."""
        return MetadataFrame(self.tables[name].values())

    def event_check_plan(self):
        """Run production candidate selection and frozen plan export."""
        return s7.SparkRecoveryStore.event_check_plan(self)

    def request_reports(self):
        """Run production original-byte and wrapper verification."""
        return s7.SparkRecoveryStore.request_reports(self)

    def add_check(self, value):
        """Freeze an actual catalog response and ingest it through real Bronze/Silver engines."""
        from test_sprint_04 import bundle
        from quake_pipeline.outbox import envelope
        v=MemoryVolume()
        c=CatalogCollector(CLOUD,QueueHTTP([Response(200,json_bytes(value))]),v,'tracked',
            now=lambda:NOW,sleeper=lambda s:None)
        report=c.collect({'eventid':value['id']},'EVENT_CHECK','run',NOW,NOW)
        self.files.update(v.files)
        manifest=inventory(v)[0]
        self.tables['ops.captures'][manifest['capture_id']]={**manifest,'fetched_at':NOW,
            'response_uri':s4.BRONZE_ROOT+'/'+manifest['response_relative_path'],
            'manifest_uri':s4.BRONZE_ROOT+'/captures/'+manifest['collection_id']+'/'+manifest['capture_id']+'/ready.json',
            'source_generated_at':NOW,'state':'PUBLISH_ACKED','accounted_item_count':0}
        offset=sum(r['row_count'] for r in self.tables['ops.batches'].values())
        evidence=bundle(self,[envelope(settings(),manifest,0,value)],offset)
        m,data,definition,raw=evidence
        self.files[m['records_relative_path']]=data
        self.files[m['records_relative_path'].rsplit('/',1)[0]+'/manifest.json']=raw
        self.files['handoff/'+m['stream_id']+'/definition.json']=s4.bronze_json(definition)
        self.tables['ops.batches'][m['batch_id']]['landed_at']=NOW
        s4.BronzeEngine(self,'bronze-target').process([evidence])
        s4.BronzeEngine(self,'account-target').account_captures()
        s5.SilverEngine(self,'silver-target').process([self.tables['ops.batches'][m['batch_id']]])
        return report


class ReconciliationIntegrationTests(unittest.TestCase):
    """Check source coverage against real frozen captures and actual business transformations."""

    def lake(self):
        """Prepare one accounted event and all three selected-feed checks."""
        store=ReconciliationLake()
        batch,_=store.add_bronze([event()]);s5.SilverEngine(store,'silver').process([batch]);store.ready_sources()
        return store

    def test_missing_tracked_check_blocks_certification(self):
        """Accounted feeds alone do not certify a previously selected display candidate."""
        store=self.lake()
        result=s7.SparkRecoveryStore.reconcile(store,'check')
        self.assertEqual(result['status'],'BLOCKED')
        self.assertTrue(any('TRACKED_EVENT_UNVERIFIED:' in issue for r in store.rows('ops.reconciliations') for issue in r['issues']))

    def test_real_catalog_capture_reconciles_after_silver(self):
        """A newer tracked lookup passes Kafka-compatible ingestion and coverage proof."""
        store=self.lake();store.add_check(event(updated=2000))
        result=s7.SparkRecoveryStore.reconcile(store,'check')
        self.assertEqual(result['status'],'SUCCESS')
        self.assertEqual(store.rows('ops.source_checks')[0]['outcome'],'ACCOUNTED')
        self.assertEqual(len(store.rows('silver.events_current')),1)

    def test_original_and_wrapper_disagreement_blocks_certification(self):
        """Rehashing changed original bytes cannot conceal a different frozen event feature."""
        store=self.lake();report=store.add_check(event(updated=2000))
        changed=json_bytes(event(updated=3000))
        store.files[report['original_relative_path']]=changed
        report['original_sha256']=digest(changed)
        store.files['reconciliation/requests/'+report['request_id']+'/ready.json']=json_bytes(report)
        with self.assertRaises(s7.RecoveryError): s7.SparkRecoveryStore.reconcile(store,'check')

    def test_backlog_stays_visible_even_with_fresh_tracked_check(self):
        """One accounted event check cannot hide another unprocessed saved response."""
        store=self.lake();store.add_check(event(updated=2000))
        from test_sprint_04 import capture
        capture(store,[event(index=2)],'pending',state='CAPTURED')
        result=s7.SparkRecoveryStore.reconcile(store,'check')
        self.assertEqual(result['status'],'BLOCKED')
        self.assertIn('PENDING_CAPTURES',store.rows('ops.reconciliations')[0]['issues'])

    def test_plan_mode_prepares_work_without_claiming_success(self):
        """The preparation Job exports request IDs without certifying coverage."""
        store=self.lake()
        result=s7.SparkRecoveryStore.reconcile(store,'plan','plan')
        self.assertEqual(result['mode'],'plan')
        self.assertEqual(store.rows('ops.reconciliations'),[])


class IntegrationBoundaryTests(unittest.TestCase):
    """Check the new lifecycle boundary against the existing Kafka and publication paths."""

    def retired(self):
        """Seal a genuine Kafka landing and retain a matching retirement proof."""
        volume,kafka,stream=MemoryVolume(),MemoryKafka(),settings()
        bind_stream(volume,kafka,stream)
        frozen(volume,[feature()])
        recover_captures(CLOUD,QueueHTTP([]),volume,kafka,stream,lambda e:None)
        land(volume,kafka,stream,'run',lambda e:None)
        path=next(p for p in volume.files if '/batches/' in p and p.endswith('/manifest.json'))
        m=decode_json(volume.files[path]);bid=m['batch_id']
        receipt=json_bytes({'batch_id':bid,'state':'SILVER_COMMITTED','manifest_sha256':digest(volume.files[path])})
        volume.files['receipts/silver/'+bid+'.json']=receipt
        volume.files['receipts/retired/batches/'+bid+'.json']=json_bytes({'batch_id':bid,'state':'RETIRED',
            'manifest_sha256':digest(volume.files[path]),'silver_receipt_sha256':digest(receipt)})
        del volume.files[m['records_relative_path']]
        return volume,kafka,stream

    def test_retired_landing_does_not_look_like_corruption(self):
        """Routine Kafka recovery recognizes intentional payload expiry."""
        volume,kafka,stream=self.retired()
        self.assertEqual(recover(volume,kafka,stream,lambda e:None),[])

    def test_retired_landing_still_rejects_checkpoint_regression(self):
        """A retirement receipt does not permit resetting Kafka behind expired evidence."""
        volume,kafka,stream=self.retired();kafka.commits[0]=0
        with self.assertRaises(ContractError):recover(volume,kafka,stream,lambda e:None)

    def test_reconciliation_issues_are_pinned_into_gold(self):
        """A complete delivery ledger cannot override an unresolved tracked-event check."""
        store=LakeStore();batch,_=store.add_bronze([event()]);s5.SilverEngine(store,'s').process([batch]);store.ready_sources()
        captures,batches,rejects=store.coverage_inputs();rejects['reconciliation_issues']=['TRACKED_EVENT_UNVERIFIED:eq0']
        rejects['historical_dates']=['2026-09-01']
        issues,scope,_=s6.gold_coverage(captures,batches,rejects,NOW)
        self.assertIn('TRACKED_EVENT_UNVERIFIED:eq0',issues)
        self.assertEqual(scope['historical_dates'],['2026-09-01'])

    def test_expired_publication_cannot_be_retried(self):
        """Output expiry requires a new candidate rather than silently rebuilding old pins."""
        store=LakeStore();batch,_=store.add_bronze([event()]);s5.SilverEngine(store,'s').process([batch]);store.ready_sources()
        pid=s6.GoldEngine(store,'g').process()['publication_id']
        for row in store.tables['ops.publications'].values():row['state']='EXPIRED'
        with self.assertRaises(s6.GoldError):s6.GoldEngine(store,'retry').process(retry_id=pid)


class CatalogCountContractTests(unittest.TestCase):
    """Verify the exact count-response structure observed from the official USGS endpoint."""

    def test_geojson_count_object_is_supported(self):
        """The documented format=geojson count object can complete a zero-result slice."""
        count=json_bytes({'count':0,'maxAllowed':20000})
        http=QueueHTTP([Response(200,count),Response(204,b''),Response(200,count)])
        collector=CatalogCollector(CLOUD,http,MemoryVolume(),'count-object',now=lambda:NOW,sleeper=lambda s:None)
        rows=collector.backfill(NOW.replace(hour=0)-timedelta(days=1),NOW.replace(hour=0),'r')
        self.assertEqual(rows[0]['count_after'],0)
        self.assertIn('format=geojson',http.calls[0][2])

    def test_invalid_count_object_is_rejected(self):
        """Missing, string or boolean counts cannot masquerade as catalog completeness."""
        from quake_pipeline.reconciliation import catalog_count
        for value in ({'maxAllowed':20000},{'count':'1'},{'count':True},{'count':-1}):
            with self.assertRaises(ContractError):catalog_count(json_bytes(value))


if __name__ == '__main__':
    unittest.main()
