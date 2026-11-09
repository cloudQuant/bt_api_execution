"""Same-journal durable host for the optional SDK Gateway delegate client."""
from __future__ import annotations

import json
import time
from dataclasses import asdict

from .contracts import canonical_json
from .errors import IntentConflictError, InvalidStateTransition


class ExecutionGatewayDelegateHost:
    def __init__(self, authority):
        from .parent_authority import ParentAuthority
        if type(authority) is not ParentAuthority:
            raise ValueError('invalid execution parent authority')
        self.authority = authority

    def _row(self, cursor, key):
        return cursor.execute('SELECT * FROM execution_parent_delegates WHERE scope_key=? AND request_key=?',
                              (self.authority.parent_scope_key, key)).fetchone()

    @staticmethod
    def _intent_json(intent):
        return canonical_json({'route': asdict(intent.route), 'command': intent.command.to_mapping(),
                               'canonical_payload': intent.canonical_payload, 'ttl_seconds': intent.ttl_seconds})

    @staticmethod
    def _intent(raw):
        from bt_api_base.gateway_delegation_protocol import GatewayDelegatedCommand
        from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope
        from bt_api_py.gateway_delegate_service_client import GatewayDelegateIntent, GatewayDelegateRoute
        value = json.loads(raw)
        route = value['route']
        identity = route['identity']
        identity['role'] = WireRole(identity['role'])
        route['identity'] = WireIdentity(**identity)
        route['scope'] = WireScope(**route['scope'])
        return GatewayDelegateIntent(GatewayDelegateRoute(**route),
            GatewayDelegatedCommand.from_mapping(value['command']), value['canonical_payload'], value['ttl_seconds'])

    def save_delegate_intent(self, intent):
        command = intent.command
        content = self._intent_json(intent)
        a = self.authority
        with a.store._transaction() as cursor:
            a._fence(cursor)
            parent = a._row(cursor, command.parent_ref)
            spec = json.loads(parent['request_json'])
            child = cursor.execute('SELECT * FROM execution_parent_children WHERE scope_key=? AND parent_id=? AND child_id=?',
                                   (a.parent_scope_key, command.parent_ref, command.child_id)).fetchone()
            if child is None or child['state'] != 'UNKNOWN' or parent['cancel_state'] != 'NONE' or parent['gateway_state'] != 'ACCEPTED':
                raise InvalidStateTransition('delegate is not covered by an active parent claim')
            child_spec = json.loads(child['request_json'])
            from .parent_authority import _decimal
            payload = json.loads(intent.canonical_payload)
            price = _decimal(payload.get('price'), 'child price', positive=True)
            limit = _decimal(spec['limit_price'], 'parent price', positive=True)
            if (child_spec['request_key'] != command.request_key or
                    child_spec.get('payload_digest') != command.payload_digest or
                    command.actor_kind != 'execution' or command.account_scope != spec['account_scope'] or
                    command.strategy_scope != spec['strategy_scope'] or
                    command.execution_domain_ref != spec['execution_domain_ref'] or
                    command.instrument.to_dict() != spec['instrument'] or command.grant_revision != spec['grant_revision'] or
                    _decimal(payload.get('quantity'), 'child quantity', positive=True) != _decimal(child_spec['quantity'], 'child quantity', positive=True) or
                    payload.get('side') != spec['side'] or payload.get('offset') != child_spec['offset'] or
                    payload.get('quantity_unit') != spec['quantity_unit'] or payload.get('order_type') != 'limit' or
                    (spec['side'] == 'buy' and price > limit) or (spec['side'] == 'sell' and price < limit) or
                    time.time_ns() >= spec['expires_at_ns'] or
                    a.admission('dispatch_child', {'parent': spec, 'child': child_spec}) is not True):
                raise InvalidStateTransition('delegate identity differs from parent or child')
            old = self._row(cursor, command.request_key)
            if old is not None:
                if old['intent_json'] != content:
                    raise IntentConflictError('delegate key was reused')
                return self._intent(old['intent_json'])
            cursor.execute('INSERT INTO execution_parent_delegates VALUES (?,?,?,NULL,NULL)',
                           (a.parent_scope_key, command.request_key, content))
        return intent

    def load_delegate_intent(self, request_key):
        a = self.authority
        with a.store._read_transaction() as cursor:
            row = self._row(cursor, request_key)
            return None if row is None else self._intent(row['intent_json'])

    def save_delegate_observation(self, request_key, observation):
        from bt_api_py.gateway_delegate_service_client import GatewayDelegateObservation
        if type(observation) is not GatewayDelegateObservation or observation.request_key != request_key:
            raise InvalidStateTransition('invalid delegate observation')
        content = canonical_json(asdict(observation))
        a = self.authority
        with a.store._transaction() as cursor:
            a._fence(cursor)
            row = self._row(cursor, request_key)
            if row is None:
                raise InvalidStateTransition('delegate intent missing')
            intent = self._intent(row['intent_json'])
            cmd = intent.command
            if (observation.account_scope != cmd.account_scope or observation.strategy_scope != cmd.strategy_scope or
                    observation.actor_principal != cmd.actor_principal or observation.digest != cmd.payload_digest):
                raise InvalidStateTransition('delegate observation identity mismatch')
            if row['observation_json']:
                old = json.loads(row['observation_json'])
                if (observation.reservation_id != old['reservation_id'] or observation.version < old['version'] or
                        (observation.version == old['version'] and content != row['observation_json'])):
                    raise InvalidStateTransition('delegate observation conflict')
            cursor.execute('UPDATE execution_parent_delegates SET observation_json=? WHERE scope_key=? AND request_key=?',
                           (content, a.parent_scope_key, request_key))
        return observation

    def load_delegate_observation(self, request_key):
        from bt_api_py.gateway_delegate_service_client import GatewayDelegateObservation
        a = self.authority
        with a.store._read_transaction() as cursor:
            row = self._row(cursor, request_key)
            return None if row is None or row['observation_json'] is None else GatewayDelegateObservation(**json.loads(row['observation_json']))

    def save_delegate_dispatch_marker(self, request_key, marker):
        if type(marker) is not tuple or len(marker) != 4:
            raise InvalidStateTransition('invalid delegate dispatch marker')
        a = self.authority
        with a.store._transaction() as cursor:
            a._fence(cursor)
            row = self._row(cursor, request_key)
            if row is None or row['observation_json'] is None:
                raise InvalidStateTransition('delegate reservation missing')
            intent = self._intent(row['intent_json'])
            obs = json.loads(row['observation_json'])
            if (obs['state'] != 'RESERVED' or marker != (obs['reservation_id'], obs['digest'], obs['version'], intent.command.digest)):
                raise InvalidStateTransition('delegate marker does not match reservation')
            parent = a._row(cursor, intent.command.parent_ref)
            spec = json.loads(parent['request_json'])
            child = cursor.execute('SELECT request_json FROM execution_parent_children WHERE scope_key=? AND parent_id=? AND child_id=?',
                                   (a.parent_scope_key, intent.command.parent_ref, intent.command.child_id)).fetchone()
            if (parent['cancel_state'] != 'NONE' or parent['gateway_state'] != 'ACCEPTED' or
                    time.time_ns() >= spec['expires_at_ns'] or child is None or
                    a.admission('dispatch_child', {'parent': spec, 'child': json.loads(child['request_json'])}) is not True):
                raise InvalidStateTransition('parent dispatch fenced')
            content = canonical_json(marker)
            if row['dispatch_marker_json'] is not None and row['dispatch_marker_json'] != content:
                raise IntentConflictError('delegate marker conflict')
            cursor.execute('UPDATE execution_parent_delegates SET dispatch_marker_json=? WHERE scope_key=? AND request_key=?',
                           (content, a.parent_scope_key, request_key))
        return marker

    def load_delegate_dispatch_marker(self, request_key):
        a = self.authority
        with a.store._read_transaction() as cursor:
            row = self._row(cursor, request_key)
            return None if row is None or row['dispatch_marker_json'] is None else tuple(json.loads(row['dispatch_marker_json']))

    def _cancel_row(self, cursor, operation_id):
        return cursor.execute('SELECT * FROM execution_parent_child_cancels WHERE scope_key=? AND operation_id=?',
                              (self.authority.parent_scope_key, operation_id)).fetchone()

    def save_delegate_cancel_intent(self, plan):
        from bt_api_py.gateway_delegate_service_client import GatewayDelegateActorCancelIntent
        from .contracts import payload_sha256
        if type(plan) is not GatewayDelegateActorCancelIntent:
            raise InvalidStateTransition('invalid delegate cancellation intent')
        a = self.authority
        content = canonical_json(asdict(plan))
        with a.store._transaction() as cursor:
            a._fence(cursor)
            row = self._cancel_row(cursor, plan.operation_id)
            if row is None:
                raise InvalidStateTransition('Gateway did not freeze this cancellation')
            target = json.loads(row['frozen_json'])
            delegate = self._row(cursor, plan.request_key)
            if delegate is None or delegate['observation_json'] is None:
                raise InvalidStateTransition('delegate source unavailable')
            intent = self._intent(delegate['intent_json'])
            obs = json.loads(delegate['observation_json'])
            if (target.get('phase') not in {'CANCEL_PENDING', 'WAIT_DISPATCH'} or
                    obs['state'] != 'SENT' or
                    target.get('native_identity') is None or
                    target.get('request_key') != plan.request_key or target.get('reservation_id') != plan.reservation_id or
                    target.get('payload_digest') != plan.payload_digest or target.get('target_principal') != plan.actor_principal or
                    payload_sha256(target['native_identity']) != plan.native_identity_digest or
                    intent.command.digest != plan.command_digest or intent.command.parent_ref != row['parent_id'] or
                    intent.command.child_id != row['child_id'] or obs['reservation_id'] != plan.reservation_id or
                    obs['digest'] != plan.payload_digest or
                    a.admission('cancel_child', {'target': target, 'plan': asdict(plan)}) is not True):
                raise InvalidStateTransition('cancel intent differs from frozen Gateway identity')
            if row['intent_json'] is not None and row['intent_json'] != content:
                raise IntentConflictError('cancel operation id was reused')
            cursor.execute('UPDATE execution_parent_child_cancels SET intent_json=? WHERE scope_key=? AND operation_id=?',
                           (content, a.parent_scope_key, plan.operation_id))
        return plan

    def load_delegate_cancel_intent(self, operation_id):
        from bt_api_py.gateway_delegate_service_client import GatewayDelegateActorCancelIntent
        a = self.authority
        with a.store._read_transaction() as cursor:
            row = self._cancel_row(cursor, operation_id)
            return None if row is None or row['intent_json'] is None else GatewayDelegateActorCancelIntent(**json.loads(row['intent_json']))

    def save_delegate_cancel_observation(self, operation_id, observation):
        from bt_api_py.gateway_delegated_cancel import GatewayDelegatedCancelObservation
        if type(observation) is not GatewayDelegatedCancelObservation or observation.operation_id != operation_id:
            raise InvalidStateTransition('invalid cancellation observation')
        a = self.authority
        content = canonical_json(asdict(observation))
        with a.store._transaction() as cursor:
            a._fence(cursor)
            row = self._cancel_row(cursor, operation_id)
            if row is None or row['intent_json'] is None:
                raise InvalidStateTransition('cancellation intent unavailable')
            plan = json.loads(row['intent_json'])
            if (observation.request_key != plan['request_key'] or observation.target_reservation_id != plan['reservation_id'] or
                    observation.target_digest != plan['payload_digest'] or observation.requester_principal != plan['actor_principal'] or
                    observation.target_actor_principal != plan['actor_principal']):
                raise InvalidStateTransition('cancellation observation identity mismatch')
            if row['observation_json'] is not None:
                old = json.loads(row['observation_json'])
                if (observation.attempt_id != old['attempt_id'] or observation.version < old['version'] or
                        (observation.version == old['version'] and content != row['observation_json'])):
                    raise InvalidStateTransition('cancellation observation conflict')
            cursor.execute('UPDATE execution_parent_child_cancels SET observation_json=? WHERE scope_key=? AND operation_id=?',
                           (content, a.parent_scope_key, operation_id))
        return observation

    def load_delegate_cancel_observation(self, operation_id):
        from bt_api_py.gateway_delegated_cancel import GatewayDelegatedCancelObservation
        a = self.authority
        with a.store._read_transaction() as cursor:
            row = self._cancel_row(cursor, operation_id)
            return None if row is None or row['observation_json'] is None else GatewayDelegatedCancelObservation(**json.loads(row['observation_json']))

    def save_delegate_cancel_marker(self, operation_id, marker):
        if type(marker) is not tuple or len(marker) != 5:
            raise InvalidStateTransition('invalid cancellation marker')
        a = self.authority
        with a.store._transaction() as cursor:
            a._fence(cursor)
            row = self._cancel_row(cursor, operation_id)
            if row is None or row['intent_json'] is None:
                raise InvalidStateTransition('cancellation intent unavailable')
            plan, target = json.loads(row['intent_json']), json.loads(row['frozen_json'])
            expected = (plan['request_key'], plan['reservation_id'], plan['payload_digest'], plan['command_digest'], plan['native_identity_digest'])
            if marker != expected or a.admission('cancel_child', {'target': target, 'plan': plan}) is not True:
                raise InvalidStateTransition('cancellation marker fenced')
            content = canonical_json(marker)
            if row['dispatch_marker_json'] is not None and row['dispatch_marker_json'] != content:
                raise IntentConflictError('cancellation marker conflict')
            cursor.execute('UPDATE execution_parent_child_cancels SET dispatch_marker_json=? WHERE scope_key=? AND operation_id=?',
                           (content, a.parent_scope_key, operation_id))
        return marker

    def load_delegate_cancel_marker(self, operation_id):
        a = self.authority
        with a.store._read_transaction() as cursor:
            row = self._cancel_row(cursor, operation_id)
            return None if row is None or row['dispatch_marker_json'] is None else tuple(json.loads(row['dispatch_marker_json']))

    def save_delegate_release_intent(self, plan):
        from bt_api_py.gateway_delegate_service_client import GatewayDelegateActorReleaseIntent
        if type(plan) is not GatewayDelegateActorReleaseIntent:
            raise InvalidStateTransition('invalid delegate release intent')
        a = self.authority
        content = canonical_json(asdict(plan))
        with a.store._transaction() as cursor:
            a._fence(cursor)
            row = self._cancel_row(cursor, plan.operation_id)
            if row is None:
                raise InvalidStateTransition('Gateway did not freeze this release')
            target = json.loads(row['frozen_json'])
            delegate = self._row(cursor, plan.request_key)
            if delegate is None or delegate['observation_json'] is None:
                raise InvalidStateTransition('delegate source unavailable')
            intent = self._intent(delegate['intent_json'])
            obs = json.loads(delegate['observation_json'])
            if (target.get('phase') != 'RELEASE_PENDING' or target.get('native_identity') is not None or
                    target.get('request_key') != plan.request_key or target.get('reservation_id') != plan.reservation_id or
                    target.get('payload_digest') != plan.payload_digest or target.get('target_principal') != plan.actor_principal or
                    target.get('target_version') != plan.expected_version or
                    intent.command.digest != plan.command_digest or intent.command.parent_ref != row['parent_id'] or
                    intent.command.child_id != row['child_id'] or obs['state'] != 'RESERVED' or
                    obs['version'] != plan.expected_version or obs['reservation_id'] != plan.reservation_id or
                    obs['digest'] != plan.payload_digest or
                    a.admission('release_child', {'target': target, 'plan': asdict(plan)}) is not True):
                raise InvalidStateTransition('release intent differs from frozen Gateway identity')
            if row['intent_json'] is not None and row['intent_json'] != content:
                raise IntentConflictError('release operation id was reused')
            cursor.execute('UPDATE execution_parent_child_cancels SET intent_json=? WHERE scope_key=? AND operation_id=?',
                           (content, a.parent_scope_key, plan.operation_id))
        return plan

    def load_delegate_release_intent(self, operation_id):
        from bt_api_py.gateway_delegate_service_client import GatewayDelegateActorReleaseIntent
        a = self.authority
        with a.store._read_transaction() as cursor:
            row = self._cancel_row(cursor, operation_id)
            return None if row is None or row['intent_json'] is None else GatewayDelegateActorReleaseIntent(**json.loads(row['intent_json']))

    def save_delegate_release_marker(self, operation_id, marker):
        if type(marker) is not tuple or len(marker) != 5:
            raise InvalidStateTransition('invalid release marker')
        a = self.authority
        with a.store._transaction() as cursor:
            a._fence(cursor)
            row = self._cancel_row(cursor, operation_id)
            if row is None or row['intent_json'] is None:
                raise InvalidStateTransition('release intent unavailable')
            plan, target = json.loads(row['intent_json']), json.loads(row['frozen_json'])
            expected = (plan['request_key'], plan['reservation_id'], plan['payload_digest'], plan['command_digest'], plan['expected_version'])
            if marker != expected or a.admission('release_child', {'target': target, 'plan': plan}) is not True:
                raise InvalidStateTransition('release marker fenced')
            content = canonical_json(marker)
            if row['dispatch_marker_json'] is not None and row['dispatch_marker_json'] != content:
                raise IntentConflictError('release marker conflict')
            cursor.execute('UPDATE execution_parent_child_cancels SET dispatch_marker_json=? WHERE scope_key=? AND operation_id=?',
                           (content, a.parent_scope_key, operation_id))
        return marker

    def load_delegate_release_marker(self, operation_id):
        return self.load_delegate_cancel_marker(operation_id)
