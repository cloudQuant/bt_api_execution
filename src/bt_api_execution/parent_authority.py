"""Durable parent admission before independently owned Risk and Gateway effects.

The injected ports are authenticated composition boundaries.  They must verify
remote replies and current strategy delegation; a JSON assertion is not proof.
The coordinator never retries an uncertain remote mutation.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Mapping

from .contracts import ExecutionScope, canonical_json, payload_sha256
from .errors import ContractValidationError, IntentConflictError, InvalidStateTransition
from .store import SqliteExecutionStore, WriterLease


def _text(value: Any, field: str) -> str:
    if (type(value) is not str or not value or value != value.strip() or not value.isprintable() or
            len(value.encode('utf-8', 'strict')) > 256):
        raise ContractValidationError('invalid ' + field)
    return value


def _decimal(value: Any, field: str, *, positive: bool = False) -> Decimal:
    if type(value) is not str or re.fullmatch(r'[0-9]{1,42}(?:\.[0-9]{1,42})?', value) is None:
        raise ContractValidationError('invalid ' + field)
    try:
        number = Decimal(value)
    except InvalidOperation as error:
        raise ContractValidationError('invalid ' + field) from error
    if not number.is_finite() or number < 0 or (positive and not number):
        raise ContractValidationError('invalid ' + field)
    return number


def _validate_spec(spec: Mapping[str, Any], scope: ExecutionScope) -> dict[str, Any]:
    if type(spec) is not dict:
        raise ContractValidationError('parent specification must be a dict')
    required = {'parent_ref', 'registration_operation_id', 'risk_operation_id', 'account_scope',
                'strategy_scope', 'execution_domain_ref', 'instrument', 'quantity',
                'quantity_unit', 'side', 'offset', 'limit_price', 'creation_trading_day',
                'allowed_trading_days', 'expires_at_ns', 'grant_revision', 'plan_digest',
                'plan_version', 'target_position_ref', 'risk_selected', 'time_in_force'}
    if set(spec) != required:
        raise ContractValidationError('incomplete parent specification')
    for name in ('parent_ref', 'registration_operation_id', 'risk_operation_id', 'account_scope',
                 'strategy_scope', 'execution_domain_ref', 'quantity_unit', 'plan_digest',
                 'plan_version', 'creation_trading_day', 'grant_revision'):
        _text(spec[name], name)
    if len(spec['plan_digest']) != 64 or any(c not in '0123456789abcdef' for c in spec['plan_digest']):
        raise ContractValidationError('invalid plan digest')
    if spec['time_in_force'] not in {'DAY', 'GTD'}:
        raise ContractValidationError('invalid parent time in force')
    try:
        from bt_api_base.market_protocol import MarketInstrumentKey
        if MarketInstrumentKey.from_dict(spec['instrument']).to_dict() != spec['instrument']:
            raise ValueError
    except Exception as error:
        raise ContractValidationError('invalid instrument identity') from error
    if spec['side'] not in {'buy', 'sell'} or spec['offset'] not in {'open', 'close', 'close_today', 'close_yesterday'}:
        raise ContractValidationError('invalid side or offset')
    if spec['offset'] != 'open':
        _text(spec['target_position_ref'], 'target_position_ref')
    elif spec['target_position_ref'] is not None:
        raise ContractValidationError('open parent cannot bind a position target')
    _decimal(spec['quantity'], 'quantity', positive=True)
    _decimal(spec['limit_price'], 'limit_price', positive=True)
    days = spec['allowed_trading_days']
    if (type(days) is not list or not 1 <= len(days) <= 366 or
            any(type(day) is not str or re.fullmatch(r'[0-9]{8}', day) is None for day in days) or
            days != sorted(set(days)) or spec['creation_trading_day'] != days[0] or
            (spec['time_in_force'] == 'DAY' and len(days) != 1)):
        raise ContractValidationError('invalid trading days')
    try:
        for day in days:
            datetime.strptime(day, '%Y%m%d')
    except ValueError as error:
        raise ContractValidationError('invalid trading day') from error
    if type(spec['expires_at_ns']) is not int or not 0 < spec['expires_at_ns'] < 2**63:
        raise ContractValidationError('invalid parent deadline')
    if type(spec['risk_selected']) is not bool:
        raise ContractValidationError('invalid parent authorization')
    try:
        return json.loads(canonical_json(spec))
    except (TypeError, ValueError) as error:
        raise ContractValidationError('parent specification is not canonical') from error


class ParentAuthority:
    """One existing Execution journal, account writer fence, and trusted ports.

    ``admission`` verifies live authenticated input and current delegation.
    The two remote owner ports implement ``register_parent(spec, operation_id)``,
    ``query_operation(spec, operation_id)`` and ``verify(result, spec, operation_id)``.
    The Gateway port additionally implements ``cancel_parent(spec, operation_id,
    expected_version)``.  verify must return the owner state (or None when it
    cannot establish it), and is responsible for validating response provenance,
    scope, original operation, content and generation. Missing ports fail closed.
    """
    def __init__(self, store: SqliteExecutionStore, scope: ExecutionScope,
                 lease: WriterLease, *, admission, gateway=None, risk=None):
        if type(store) is not SqliteExecutionStore or type(scope) is not ExecutionScope:
            raise ContractValidationError('invalid parent journal')
        if not callable(admission):
            raise ContractValidationError('parent authority requires trusted composition')
        self.store, self.scope, self.lease = store, scope, lease
        self.parent_scope_key = ExecutionScope(scope.provider, scope.environment, scope.account_ref, scope.strategy_id).key
        self.admission, self.gateway, self.risk = admission, gateway, risk
        self._owner_pid = os.getpid()
        self._owner_thread = threading.current_thread()
        self._dispatch_claims = {}

    def bind_gateway(self, gateway):
        """Bind the SDK port once after constructing it with this object as host."""
        if gateway is None or self.gateway is not None:
            raise InvalidStateTransition('gateway authority already bound or unavailable')
        self.gateway = gateway

    def _fence(self, cursor):
        self.store._assert_active_writer_lease(cursor, self.scope, self.lease)

    def _row(self, cursor, parent_id):
        row = cursor.execute('SELECT * FROM execution_parent_records WHERE scope_key=? AND parent_id=?',
                             (self.parent_scope_key, parent_id)).fetchone()
        if row is None:
            raise InvalidStateTransition('unknown parent')
        return row

    def get(self, parent_id):
        with self.store._read_transaction() as cursor:
            row = self._row(cursor, _text(parent_id, 'parent_id'))
            return dict(row)

    def accept(self, spec):
        value = _validate_spec(spec, self.scope)
        if self.admission('accept', value) is not True:
            raise InvalidStateTransition('parent admission denied')
        content, digest, now = canonical_json(value), payload_sha256(value), time.time_ns()
        with self.store._transaction() as cursor:
            self._fence(cursor)
            row = cursor.execute('SELECT * FROM execution_parent_records WHERE scope_key=? AND parent_id=?',
                                 (self.parent_scope_key, value['parent_ref'])).fetchone()
            if row is not None:
                if row['request_json'] != content or row['request_sha256'] != digest:
                    raise IntentConflictError('parent id was reused with different content')
                return dict(row)
            if value['expires_at_ns'] <= time.time_ns():
                raise ContractValidationError('parent deadline has expired')
            cursor.execute('INSERT INTO execution_parent_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                           (self.parent_scope_key, self.scope.account_key, value['parent_ref'], content, digest,
                            value['risk_operation_id'], value['registration_operation_id'],
                            'PENDING' if value['risk_selected'] else 'NOT_SELECTED',
                            'PENDING', 'NONE', None, None, None, None, None, now, now))
            return dict(self._row(cursor, value['parent_ref']))

    def _claim(self, parent_id, column, prerequisite):
        with self.store._transaction() as cursor:
            self._fence(cursor)
            row = self._row(cursor, parent_id)
            spec = json.loads(row['request_json'])
            if row['cancel_state'] != 'NONE' or self.admission('register', spec) is not True:
                raise InvalidStateTransition('parent registration fenced')
            if time.time_ns() >= spec['expires_at_ns'] or not prerequisite(row):
                raise InvalidStateTransition('parent registration prerequisite missing')
            if row[column] != 'PENDING':
                raise InvalidStateTransition('parent operation already attempted')
            cursor.execute(f'UPDATE execution_parent_records SET {column}=?, updated_at_ns=? WHERE scope_key=? AND parent_id=?',
                           ('UNKNOWN', time.time_ns(), self.parent_scope_key, parent_id))
        if column == 'gateway_state':
            self._dispatch_claims[(parent_id, spec['registration_operation_id'])] = ('REGISTER', None)
        return spec

    def prepare_parent_dispatch(self, gateway_spec, operation_id, kind, expected_version):
        """Consume the in-process right to issue one already journaled RPC."""
        if os.getpid() != self._owner_pid or threading.current_thread() is not self._owner_thread:
            return False
        if type(gateway_spec) is not dict or kind not in {'REGISTER', 'CANCEL'}:
            return False
        parent_id = gateway_spec.get('parent_ref')
        token = (parent_id, operation_id)
        if self._dispatch_claims.pop(token, None) != (kind, expected_version):
            return False
        try:
            with self.store._transaction() as cursor:
                self._fence(cursor)
                row = self._row(cursor, parent_id)
                spec = json.loads(row['request_json'])
                wire_spec = {k: v for k, v in spec.items() if k not in {'risk_selected', 'risk_operation_id'}}
                if wire_spec != gateway_spec or self.admission('owner_dispatch', spec) is not True:
                    return False
                if kind == 'REGISTER':
                    return (expected_version is None and row['gateway_state'] == 'UNKNOWN' and
                            row['cancel_state'] == 'NONE' and
                            row['risk_state'] in {'NOT_SELECTED', 'ACCEPTED'} and
                            row['gateway_operation_id'] == operation_id)
                cancel_request = json.loads(row['cancel_request_json'] or '{}')
                return (row['cancel_state'] == 'UNKNOWN' and row['cancel_operation_id'] == operation_id and
                        cancel_request == {'operation_id': operation_id, 'expected_version': expected_version})
        except Exception:
            return False

    def _resolve(self, parent_id, column, state, receipt=None):
        with self.store._transaction() as cursor:
            self._fence(cursor)
            row = self._row(cursor, parent_id)
            if row[column] != 'UNKNOWN':
                raise InvalidStateTransition('operation is not unknown')
            if state not in {'ACCEPTED', 'REJECTED'}:
                return dict(row)
            receipt_column = 'risk_receipt_json' if column == 'risk_state' else 'gateway_receipt_json'
            cursor.execute(f'UPDATE execution_parent_records SET {column}=?, {receipt_column}=COALESCE(?,{receipt_column}), updated_at_ns=? WHERE scope_key=? AND parent_id=?',
                           (state, receipt, time.time_ns(), self.parent_scope_key, parent_id))
            return dict(self._row(cursor, parent_id))

    def register(self, parent_id):
        """Attempt each previously unattempted owner once; uncertain calls stop."""
        parent_id = _text(parent_id, 'parent_id')
        if self.gateway is None:
            raise InvalidStateTransition('Gateway authority unavailable')
        row = self.get(parent_id)
        if row['risk_state'] == 'PENDING':
            if self.risk is None:
                return row
            spec = self._claim(parent_id, 'risk_state', lambda r: True)
            try:
                result = self.risk.register_parent(spec, spec['risk_operation_id'])
                state = self.risk.verify(result, spec, spec['risk_operation_id'])
            except Exception:
                return self.get(parent_id)
            receipt = canonical_json(dict(result)) if state == 'ACCEPTED' else None
            self._resolve(parent_id, 'risk_state', state, receipt)
        row = self.get(parent_id)
        if row['risk_state'] not in {'NOT_SELECTED', 'ACCEPTED'} or row['gateway_state'] != 'PENDING':
            return row
        spec = self._claim(parent_id, 'gateway_state', lambda r: r['risk_state'] in {'NOT_SELECTED', 'ACCEPTED'})
        try:
            result = self.gateway.register_parent(spec, spec['registration_operation_id'])
            state = self.gateway.verify(result, spec, spec['registration_operation_id'])
            receipt = canonical_json(dict(result)) if state == 'ACCEPTED' else None
        except Exception:
            return self.get(parent_id)
        return self._resolve(parent_id, 'gateway_state', state, receipt)

    def reconcile(self, parent_id, owner):
        if owner not in {'risk', 'gateway'}:
            raise ContractValidationError('unknown parent owner')
        row = self.get(parent_id)
        spec = json.loads(row['request_json'])
        column, port = owner + '_state', self.risk if owner == 'risk' else self.gateway
        if row[column] != 'UNKNOWN' or port is None:
            raise InvalidStateTransition('parent operation is not unknown')
        operation = spec['risk_operation_id'] if owner == 'risk' else spec['registration_operation_id']
        try:
            result = port.query_operation(spec, operation)
            state = port.verify(result, spec, operation)
            receipt = canonical_json(dict(result)) if state == 'ACCEPTED' else None
        except Exception:
            return self.get(parent_id)
        return self._resolve(parent_id, column, state, receipt)

    def cancel(self, parent_id, operation_id, expected_version):
        """Fence new children durably before asking Gateway to freeze the parent."""
        parent_id, operation_id = _text(parent_id, 'parent_id'), _text(operation_id, 'operation_id')
        if type(expected_version) is not int or expected_version <= 0:
            raise ContractValidationError('invalid gateway parent version')
        if self.gateway is None:
            raise InvalidStateTransition('Gateway authority unavailable')
        with self.store._transaction() as cursor:
            self._fence(cursor)
            row = self._row(cursor, parent_id)
            spec = json.loads(row['request_json'])
            if operation_id in {row['gateway_operation_id'], row['risk_operation_id']}:
                raise IntentConflictError('cancellation operation id conflicts with registration')
            if self.admission('cancel', spec) is not True:
                raise InvalidStateTransition('parent cancellation denied')
            if row['cancel_state'] != 'NONE':
                if (row['cancel_operation_id'] != operation_id or
                        json.loads(row['cancel_request_json'] or '{}') !=
                        {'operation_id': operation_id, 'expected_version': expected_version}):
                    raise IntentConflictError('parent already has another cancellation')
                return dict(row)
            cancel_request = canonical_json({'operation_id': operation_id, 'expected_version': expected_version})
            cursor.execute("UPDATE execution_parent_records SET cancel_state='UNKNOWN', cancel_operation_id=?, cancel_request_json=?, updated_at_ns=? WHERE scope_key=? AND parent_id=?",
                           (operation_id, cancel_request, time.time_ns(), self.parent_scope_key, parent_id))
            # PREPARED children have never crossed the durable dispatch claim.
            # Cancel those locally in the same transaction; all claimed or
            # UNKNOWN children retain their full occupancy.
            unsent = cursor.execute("SELECT child_id FROM execution_parent_children WHERE scope_key=? AND parent_id=? AND state='PREPARED'",
                                    (self.parent_scope_key, parent_id)).fetchall()
            for item in unsent:
                child_id = item['child_id']
                event_id = 'local-cancel:' + payload_sha256([parent_id, child_id, operation_id])
                evidence = canonical_json({'parent_ref': parent_id, 'child_id': child_id,
                    'event_id': event_id, 'state': 'TERMINAL', 'filled_quantity': '0',
                    'watermark_aligned': True, 'source': 'local_before_dispatch_claim'})
                cursor.execute('INSERT INTO execution_parent_child_evidence VALUES (?,?,?,?,?,?)',
                               (self.parent_scope_key, parent_id, child_id, event_id, evidence, time.time_ns()))
                cursor.execute("UPDATE execution_parent_children SET state='TERMINAL', updated_at_ns=? WHERE scope_key=? AND parent_id=? AND child_id=?",
                               (time.time_ns(), self.parent_scope_key, parent_id, child_id))
        self._dispatch_claims[(parent_id, operation_id)] = ('CANCEL', expected_version)
        try:
            result = self.gateway.cancel_parent(spec, operation_id, expected_version)
            state = self.gateway.verify(result, spec, operation_id)
        except Exception:
            return self.get(parent_id)
        return self._resolve_cancel(parent_id, state, result)

    def reconcile_cancel(self, parent_id):
        row = self.get(parent_id)
        if row['cancel_state'] != 'UNKNOWN':
            raise InvalidStateTransition('cancellation is not unknown')
        spec, operation = json.loads(row['request_json']), row['cancel_operation_id']
        try:
            result = self.gateway.query_operation(spec, operation)
            state = self.gateway.verify(result, spec, operation)
        except Exception:
            return self.get(parent_id)
        return self._resolve_cancel(parent_id, state, result)

    def _resolve_cancel(self, parent_id, state, receipt):
        with self.store._transaction() as cursor:
            self._fence(cursor)
            row = self._row(cursor, parent_id)
            if row['cancel_state'] != 'UNKNOWN':
                raise InvalidStateTransition('cancellation is not unknown')
            # ACCEPTED proves only a freeze request, never cancellation of
            # active or uncertain children, nor release of their quantities.
            if state == 'ACCEPTED':
                result = dict(receipt).get('result')
                if type(result) is not dict or type(result.get('targets')) is not list:
                    raise InvalidStateTransition('verified cancellation targets unavailable')
                spec = json.loads(row['request_json'])
                seen = set()
                for target in result['targets']:
                    if type(target) is not dict:
                        raise InvalidStateTransition('invalid frozen cancellation target')
                    child_id = _text(target.get('child_id'), 'child_id')
                    operation_id = _text(target.get('cancel_operation_id'), 'cancel_operation_id')
                    if child_id in seen or target.get('account_scope') != spec['account_scope'] or target.get('strategy_scope') != spec['strategy_scope'] or target.get('parent_ref') != parent_id:
                        raise InvalidStateTransition('frozen cancellation target scope mismatch')
                    seen.add(child_id)
                    child = cursor.execute('SELECT * FROM execution_parent_children WHERE scope_key=? AND parent_id=? AND child_id=?',
                                           (self.parent_scope_key, parent_id, child_id)).fetchone()
                    if child is None or child['state'] == 'PREPARED':
                        raise InvalidStateTransition('unmapped frozen cancellation target')
                    child_spec = json.loads(child['request_json'])
                    if target.get('request_key') != child_spec['request_key'] or target.get('payload_digest') != child_spec['payload_digest']:
                        raise InvalidStateTransition('frozen cancellation target identity mismatch')
                    frozen_json = canonical_json(target)
                    existing = cursor.execute('SELECT * FROM execution_parent_child_cancels WHERE scope_key=? AND operation_id=?',
                                              (self.parent_scope_key, operation_id)).fetchone()
                    if existing is not None and existing['frozen_json'] != frozen_json:
                        raise IntentConflictError('frozen cancellation target conflict')
                    if existing is None:
                        cursor.execute('INSERT INTO execution_parent_child_cancels VALUES (?,?,?,?,?,NULL,NULL,NULL)',
                                       (self.parent_scope_key, parent_id, child_id, operation_id, frozen_json))
                cursor.execute("UPDATE execution_parent_records SET cancel_state='REQUESTED', cancel_receipt_json=?, updated_at_ns=? WHERE scope_key=? AND parent_id=?",
                               (canonical_json(dict(receipt)), time.time_ns(), self.parent_scope_key, parent_id))
            return dict(self._row(cursor, parent_id))

    def advance_cancel_children(self, parent_id, delegate_client):
        """Drive only Gateway-frozen targets, preserving each original operation."""
        from bt_api_py.gateway_delegate_service_client import GatewayDelegateServiceClient
        from .gateway_delegate_host import ExecutionGatewayDelegateHost
        host = getattr(delegate_client, '_host', None)
        if (type(delegate_client) is not GatewayDelegateServiceClient or
                type(host) is not ExecutionGatewayDelegateHost or host.authority is not self):
            raise InvalidStateTransition('durable Gateway delegate unavailable')
        with self.store._read_transaction() as cursor:
            parent = self._row(cursor, parent_id)
            if parent['cancel_state'] != 'REQUESTED':
                raise InvalidStateTransition('parent freeze is not confirmed')
            rows = cursor.execute('SELECT * FROM execution_parent_child_cancels WHERE scope_key=? AND parent_id=? ORDER BY child_id',
                                  (self.parent_scope_key, parent_id)).fetchall()
            pending = [dict(item) for item in rows]
        results = []
        for row in pending:
            target = json.loads(row['frozen_json'])
            operation_id = row['operation_id']
            if target['phase'] == 'RELEASE_PENDING':
                if row['intent_json'] is not None:
                    released = delegate_client.actor_release_status(operation_id)
                else:
                    observed = delegate_client.actor_status(target['request_key'])
                    if (getattr(observed, 'outcome', None) != 'OBSERVED' or
                            getattr(observed, 'observation', None) is None):
                        results.append(observed)
                        continue
                    observed_child = observed.observation.command
                    if (observed_child.state != 'RESERVED' or observed.observation.native_identity_digest is not None or
                            observed_child.account_scope != target['account_scope'] or
                            observed_child.strategy_scope != target['strategy_scope'] or
                            observed_child.actor_principal != target['target_principal'] or
                            observed_child.request_key != target['request_key'] or
                            observed_child.reservation_id != target['reservation_id'] or
                            observed_child.digest != target['payload_digest'] or
                            observed_child.version != target['target_version']):
                        results.append(observed)
                        continue
                    released = delegate_client.release_actor(observed.observation, operation_id=operation_id)
                if (getattr(released, 'outcome', None) == 'OBSERVED' and
                        getattr(released, 'state', None) in {'RELEASED', 'EXPIRED'} and
                        getattr(released, 'operation_id', None) == operation_id and
                        getattr(released, 'reservation_id', None) == target['reservation_id']):
                    self._record_released_child(parent_id, row, target, released, host)
                results.append(released)
                continue
            if row['intent_json'] is not None:
                # A persisted intent is already potentially in flight even if
                # the process died before writing the dispatch marker.
                results.append(delegate_client.actor_cancel_status(operation_id))
                continue
            observed = delegate_client.actor_status(target['request_key'])
            if (getattr(observed, 'outcome', None) != 'OBSERVED' or
                    getattr(observed, 'observation', None) is None):
                results.append(observed)
                continue
            observed_child = observed.observation.command
            if (observed_child.state != 'SENT' or target['native_identity'] is None or
                    observed_child.account_scope != target['account_scope'] or
                    observed_child.strategy_scope != target['strategy_scope'] or
                    observed_child.actor_principal != target['target_principal'] or
                    observed_child.request_key != target['request_key'] or
                    observed_child.reservation_id != target['reservation_id'] or
                    observed_child.digest != target['payload_digest'] or
                    observed.observation.native_identity_digest != payload_sha256(target['native_identity'])):
                results.append(observed)
                continue
            results.append(delegate_client.cancel_actor(observed.observation, operation_id=operation_id))
        return tuple(results)

    def _record_released_child(self, parent_id, row, target, result, host):
        """A verified original release still needs the trusted account fence."""
        observation = host.load_delegate_observation(target['request_key'])
        if (observation is None or observation.state not in {'RELEASED', 'EXPIRED'} or
                observation.owner_generation != observation.reservation_generation or
                self.admission('release_proof', {'target': target, 'observation': asdict(observation),
                                                 'result': asdict(result)}) is not True):
            return
        event_id = 'gateway-release:' + payload_sha256([parent_id, row['child_id'], row['operation_id']])
        evidence = {'event_id': event_id, 'parent_ref': parent_id, 'child_id': row['child_id'],
                    'state': 'TERMINAL', 'filled_quantity': '0', 'watermark_aligned': True,
                    'source': 'gateway_fenced_release', 'target': target, 'observation': asdict(observation),
                    'result': asdict(result)}
        with self.store._transaction() as cursor:
            self._fence(cursor)
            child = cursor.execute('SELECT * FROM execution_parent_children WHERE scope_key=? AND parent_id=? AND child_id=?',
                                   (self.parent_scope_key, parent_id, row['child_id'])).fetchone()
            if child is None or child['state'] != 'UNKNOWN' or _decimal(child['filled_quantity'], 'filled') != 0:
                return
            cursor.execute('INSERT OR IGNORE INTO execution_parent_child_evidence VALUES (?,?,?,?,?,?)',
                           (self.parent_scope_key, parent_id, row['child_id'], event_id, canonical_json(evidence), time.time_ns()))
            cursor.execute("UPDATE execution_parent_children SET state='TERMINAL', updated_at_ns=? WHERE scope_key=? AND parent_id=? AND child_id=?",
                           (time.time_ns(), self.parent_scope_key, parent_id, row['child_id']))

    def prepare_child(self, parent_id, child):
        """Reserve local capacity; Gateway remains the authority for dispatch."""
        parent_id = _text(parent_id, 'parent_id')
        if type(child) is not dict:
            raise ContractValidationError('invalid child')
        for key in ('child_id', 'request_key', 'trading_day', 'offset', 'quantity', 'payload_digest'):
            if key not in child:
                raise ContractValidationError('incomplete child')
        child_id = _text(child['child_id'], 'child_id')
        _text(child['request_key'], 'request_key')
        digest_text = child['payload_digest']
        if type(digest_text) is not str or len(digest_text) != 64 or any(c not in '0123456789abcdef' for c in digest_text):
            raise ContractValidationError('invalid child payload digest')
        quantity = _decimal(child['quantity'], 'child quantity', positive=True)
        try:
            content, digest = canonical_json(child), payload_sha256(child)
        except (TypeError, ValueError) as error:
            raise ContractValidationError('invalid child content') from error
        with self.store._transaction() as cursor:
            self._fence(cursor)
            row = self._row(cursor, parent_id)
            spec = json.loads(row['request_json'])
            existing = cursor.execute('SELECT * FROM execution_parent_children WHERE scope_key=? AND parent_id=? AND child_id=?',
                                      (self.parent_scope_key, parent_id, child_id)).fetchone()
            if existing is not None:
                if existing['request_json'] != content or existing['request_sha256'] != digest:
                    raise IntentConflictError('child id was reused with different content')
                return dict(existing)
            if (row['gateway_state'] != 'ACCEPTED' or row['cancel_state'] != 'NONE' or
                    time.time_ns() >= spec['expires_at_ns'] or child['trading_day'] not in spec['allowed_trading_days'] or
                    self.admission('child', {'parent': spec, 'child': child}) is not True):
                raise InvalidStateTransition('child authority unavailable')
            if spec['time_in_force'] == 'DAY' and child['trading_day'] != spec['creation_trading_day']:
                raise InvalidStateTransition('DAY parent cannot cross trading days')
            rollover = spec['offset'] == 'close_today' and child['trading_day'] != spec['creation_trading_day']
            if rollover:
                if (child['offset'] != 'close_yesterday' or
                        child.get('target_position_ref') != spec['target_position_ref'] or
                        not child.get('mapping_version') or not child.get('provider_rule_version') or
                        _decimal(child.get('actual_closeable'), 'actual_closeable') < quantity or
                        self.admission('rollover', {'parent': spec, 'child': child}) is not True):
                    raise InvalidStateTransition('rollover evidence unavailable')
            elif child['offset'] != spec['offset']:
                raise InvalidStateTransition('child offset differs from parent')
            children = cursor.execute('SELECT quantity, filled_quantity, state FROM execution_parent_children WHERE scope_key=? AND parent_id=?',
                                      (self.parent_scope_key, parent_id)).fetchall()
            with localcontext() as context:
                context.prec = 180
                occupied = sum((_decimal(item['filled_quantity'], 'filled') if item['state'] == 'TERMINAL'
                                else _decimal(item['quantity'], 'quantity')) for item in children)
                if occupied + quantity > _decimal(spec['quantity'], 'parent quantity', positive=True):
                    raise InvalidStateTransition('parent capacity exhausted')
            cursor.execute('INSERT INTO execution_parent_children VALUES (?,?,?,?,?,?,?,?,?)',
                           (self.parent_scope_key, parent_id, child_id, content, digest,
                            child['quantity'], '0', 'PREPARED', time.time_ns()))
            return dict(cursor.execute('SELECT * FROM execution_parent_children WHERE scope_key=? AND parent_id=? AND child_id=?',
                                       (self.parent_scope_key, parent_id, child_id)).fetchone())

    def claim_child(self, parent_id, child_id):
        """Persist uncertainty before handing a child to the delegated SDK path."""
        with self.store._transaction() as cursor:
            self._fence(cursor)
            row = self._row(cursor, parent_id)
            spec = json.loads(row['request_json'])
            child = cursor.execute('SELECT * FROM execution_parent_children WHERE scope_key=? AND parent_id=? AND child_id=?',
                                   (self.parent_scope_key, parent_id, child_id)).fetchone()
            if child is None or child['state'] != 'PREPARED':
                raise InvalidStateTransition('child is not prepared')
            child_spec = json.loads(child['request_json'])
            if (spec['risk_selected'] or row['gateway_state'] != 'ACCEPTED' or row['cancel_state'] != 'NONE' or
                    time.time_ns() >= spec['expires_at_ns'] or
                    self.admission('dispatch_child', {'parent': spec, 'child': child_spec}) is not True):
                raise InvalidStateTransition('child dispatch fenced')
            cursor.execute("UPDATE execution_parent_children SET state='UNKNOWN', updated_at_ns=? WHERE scope_key=? AND parent_id=? AND child_id=?",
                           (time.time_ns(), self.parent_scope_key, parent_id, child_id))
            return child_spec

    def dispatch_child(self, parent_id, child_id, command, request, delegate_client, *, ttl_seconds=10.0):
        """Use the SDK delegated path once; all ambiguous outcomes stay occupied."""
        from bt_api_py.gateway_delegate_service_client import GatewayDelegateServiceClient
        from .gateway_delegate_host import ExecutionGatewayDelegateHost
        host = getattr(delegate_client, '_host', None)
        if (type(delegate_client) is not GatewayDelegateServiceClient or
                type(host) is not ExecutionGatewayDelegateHost or host.authority is not self):
            raise InvalidStateTransition('durable Gateway delegate unavailable')
        parent = json.loads(self.get(parent_id)['request_json'])
        if parent['risk_selected']:
            raise InvalidStateTransition('Risk child dispatch authority unavailable')
        if (getattr(command, 'parent_ref', None) != parent_id or
                getattr(command, 'child_id', None) != child_id):
            raise ContractValidationError('delegate child identity mismatch')
        child = self.claim_child(parent_id, child_id)
        if (getattr(command, 'request_key', None) != child['request_key'] or
                getattr(command, 'payload_digest', None) != child['payload_digest']):
            raise InvalidStateTransition('delegate child content mismatch')
        try:
            payload = request.to_dict()
            price = _decimal(payload['price'], 'child price', positive=True)
            limit = _decimal(parent['limit_price'], 'parent price', positive=True)
            if (payload['idempotency_key'] != child['request_key'] or
                    _decimal(payload['quantity'], 'child quantity', positive=True) != _decimal(child['quantity'], 'child quantity', positive=True) or
                    payload['side'] != parent['side'] or payload['offset'] != child['offset'] or
                    payload['quantity_unit'] != parent['quantity_unit'] or payload['order_type'] != 'limit' or
                    (parent['side'] == 'buy' and price > limit) or (parent['side'] == 'sell' and price < limit)):
                raise InvalidStateTransition('delegated order differs from parent and child')
        except (AttributeError, KeyError, TypeError) as error:
            raise ContractValidationError('invalid delegated order') from error
        result = delegate_client.reserve(command, request, ttl_seconds=ttl_seconds)
        observation = getattr(result, 'observation', None)
        if getattr(result, 'outcome', None) != 'OBSERVED' or getattr(observation, 'state', None) != 'RESERVED':
            return result
        return delegate_client.dispatch(command.request_key)

    def observe_child(self, parent_id, child_id, evidence):
        """Apply trusted cumulative evidence; an empty query never frees capacity."""
        if type(evidence) is not dict or evidence.get('parent_ref') != parent_id or evidence.get('child_id') != child_id:
            raise ContractValidationError('invalid child evidence identity')
        event_id = _text(evidence.get('event_id'), 'event_id')
        evidence_json = canonical_json(evidence)
        filled = _decimal(evidence.get('filled_quantity'), 'filled quantity')
        if evidence.get('state') not in {'ACTIVE', 'UNKNOWN', 'TERMINAL'}:
            raise ContractValidationError('invalid child evidence state')
        with self.store._transaction() as cursor:
            self._fence(cursor)
            self._row(cursor, parent_id)
            row = cursor.execute('SELECT * FROM execution_parent_children WHERE scope_key=? AND parent_id=? AND child_id=?',
                                 (self.parent_scope_key, parent_id, child_id)).fetchone()
            if row is None or row['state'] == 'PREPARED':
                raise InvalidStateTransition('child has not been dispatched')
            previous = cursor.execute('SELECT evidence_json FROM execution_parent_child_evidence WHERE scope_key=? AND parent_id=? AND child_id=? AND event_id=?',
                                      (self.parent_scope_key, parent_id, child_id, event_id)).fetchone()
            if previous is not None:
                if previous['evidence_json'] != evidence_json:
                    raise IntentConflictError('child evidence identity conflict')
                return dict(row)
            if (self.admission('child_observation', evidence) is not True or
                    filled < _decimal(row['filled_quantity'], 'stored filled') or
                    filled > _decimal(row['quantity'], 'stored quantity') or
                    (row['state'] == 'TERMINAL' and evidence['state'] != 'TERMINAL')):
                raise InvalidStateTransition('unverified or regressing child evidence')
            if evidence['state'] == 'TERMINAL' and evidence.get('watermark_aligned') is not True:
                raise InvalidStateTransition('child terminal watermark not aligned')
            cursor.execute('INSERT INTO execution_parent_child_evidence VALUES (?,?,?,?,?,?)',
                           (self.parent_scope_key, parent_id, child_id, event_id, evidence_json, time.time_ns()))
            cursor.execute('UPDATE execution_parent_children SET filled_quantity=?, state=?, updated_at_ns=? WHERE scope_key=? AND parent_id=? AND child_id=?',
                           (evidence['filled_quantity'], evidence['state'], time.time_ns(), self.parent_scope_key, parent_id, child_id))
            return dict(cursor.execute('SELECT * FROM execution_parent_children WHERE scope_key=? AND parent_id=? AND child_id=?',
                                       (self.parent_scope_key, parent_id, child_id)).fetchone())
