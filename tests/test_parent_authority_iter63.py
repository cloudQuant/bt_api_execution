from __future__ import annotations

import sqlite3
import time

import pytest

from bt_api_execution.contracts import ExecutionScope
from bt_api_execution.errors import DurableStoreError, IntentConflictError, InvalidStateTransition
from bt_api_execution.parent_authority import ParentAuthority
from bt_api_execution.store import SqliteExecutionStore


class Owner:
    def __init__(self, lose=False):
        self.lose = lose
        self.calls = []

    def register_parent(self, spec, operation_id):
        self.calls.append(('register', operation_id))
        if self.lose:
            raise TimeoutError()
        return {'state': 'ACCEPTED'}

    def query_operation(self, spec, operation_id):
        self.calls.append(('query', operation_id))
        return {'state': 'ACCEPTED'}

    def cancel_parent(self, spec, operation_id, expected_version):
        self.calls.append(('cancel', operation_id))
        return {'state': 'ACCEPTED', 'result': {'targets': []}}

    def verify(self, response, spec, operation_id):
        return response['state']


def spec(scope):
    from bt_api_base.market_protocol import MarketInstrumentKey
    return dict(parent_ref='p1', registration_operation_id='register1', risk_operation_id='risk1',
                account_scope='a', strategy_scope='s', execution_domain_ref='d', instrument=MarketInstrumentKey('ctp', 'SHFE', 'simnow', 'future', 'rb', 'rb').to_dict(),
                quantity='5', quantity_unit='LOT', side='sell', offset='close_today', limit_price='5',
                creation_trading_day='20261109', allowed_trading_days=['20261109', '20261110'],
                expires_at_ns=time.time_ns() + 60_000_000_000, grant_revision='g1',
                plan_digest='a' * 64, plan_version='v1', target_position_ref='pos',
                risk_selected=False, time_in_force='GTD')


def open_parent(path, owner, scope=None):
    scope = scope or ExecutionScope('FAKE', 'test', 'a', 's', '20261109')
    store = SqliteExecutionStore(path)
    lease = store.acquire_or_renew_lease(scope, 'writer', ttl_ns=300_000_000_000)
    return store, ParentAuthority(store, scope, lease, admission=lambda action, value: True, gateway=owner)


def test_unknown_is_not_retried_and_old_day_capacity_survives_restart(tmp_path):
    path = tmp_path / 'journal.sqlite'
    owner = Owner(lose=True)
    store, authority = open_parent(path, owner)
    parent = spec(authority.scope)
    authority.accept(parent)
    assert authority.register('p1')['gateway_state'] == 'UNKNOWN'
    authority.register('p1')
    assert owner.calls == [('register', 'register1')]
    store.close()
    scope = ExecutionScope('FAKE', 'test', 'a', 's', '20261110')
    store, authority = open_parent(path, owner, scope)
    assert authority.reconcile('p1', 'gateway')['gateway_state'] == 'ACCEPTED'
    child = dict(child_id='c1', request_key='k1', trading_day='20261109', offset='close_today', quantity='3', payload_digest='b' * 64)
    authority.prepare_child('p1', child)
    authority.claim_child('p1', 'c1')
    new_child = dict(child_id='c2', request_key='k2', trading_day='20261110', offset='close_yesterday',
                     quantity='3', target_position_ref='pos', mapping_version='m1', provider_rule_version='r1', actual_closeable='5', payload_digest='c' * 64)
    with pytest.raises(InvalidStateTransition):
        authority.prepare_child('p1', new_child)
    assert authority.cancel('p1', 'cancel1', 1)['cancel_state'] == 'REQUESTED'
    with pytest.raises(InvalidStateTransition):
        authority.prepare_child('p1', dict(new_child, quantity='2'))
    store.close()


def test_v26_migration_rejects_damaged_schema(tmp_path):
    path = tmp_path / 'journal.sqlite'
    store = SqliteExecutionStore(path)
    store.close()
    with sqlite3.connect(path) as db:
        db.execute('DROP TABLE execution_parent_children')
        db.execute('DROP TABLE execution_parent_records')
        db.execute('DROP TABLE execution_parent_delegates')
        db.execute('DROP TABLE execution_parent_child_evidence')
        db.execute('DROP TABLE execution_parent_child_cancels')
        db.execute("UPDATE execution_meta SET value='26' WHERE key='schema_version'")
    store = SqliteExecutionStore(path)
    store.close()
    with sqlite3.connect(path) as db:
        db.execute('DROP TABLE execution_parent_children')
        db.execute('DROP TABLE execution_parent_records')
        db.execute('DROP TABLE execution_parent_delegates')
        db.execute('DROP TABLE execution_parent_child_evidence')
        db.execute('DROP TABLE execution_parent_child_cancels')
        db.execute('DROP TABLE execution_outbox')
        db.execute("UPDATE execution_meta SET value='26' WHERE key='schema_version'")
    with pytest.raises(DurableStoreError):
        SqliteExecutionStore(path)


def test_sdk_parent_client_requires_and_consumes_journal_claim(tmp_path, monkeypatch):
    from bt_api_base.gateway_direct_protocol import gateway_payload_digest
    from bt_api_base.market_protocol import MarketInstrumentKey
    from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope
    from bt_api_py.gateway_delegate_service_client import GatewayDelegateRoute
    from bt_api_py.gateway_parent_client import GatewayParentServiceClient
    from bt_api_py.gateway_parent_service import REGISTER, OPERATION, CANCEL
    from bt_api_py.service_runtime_thread import ThreadedServiceClientRuntime, BindingSnapshot, RuntimeSnapshot

    identity = WireIdentity(WireRole.SERVICE, 'execution', 'exec-actor')
    wire_scope = WireScope('d', account_ref='a', strategy_ref='s', run_id='run')
    route = GatewayDelegateRoute('upstream.direct_write', 'gateway', 'instance', 'scope', wire_scope, identity)
    runtime = ThreadedServiceClientRuntime(lambda: None)
    binding = BindingSnapshot('registration', 'epoch', True, route.mount, route.service_id,
                              route.instance_id, route.scope_ref, wire_scope,
                              frozenset({REGISTER, OPERATION, CANCEL}), True)
    snapshot = RuntimeSnapshot('running', True, True, None, (binding,), 0, 0, identity, 'run')
    monkeypatch.setattr(runtime, 'status', lambda: snapshot)
    store, authority = open_parent(tmp_path / 'journal.sqlite', Owner())
    authority.gateway = None
    client = GatewayParentServiceClient(runtime, route=route, host=authority)
    authority.bind_gateway(client)
    parent = spec(authority.scope)
    parent['instrument'] = MarketInstrumentKey('ctp', 'SHFE', 'simnow', 'future', 'rb', 'rb').to_dict()
    parent['quantity_unit'] = 'lots'
    calls = []
    def call(_mount, operation, args, **_kwargs):
        calls.append(operation)
        assert authority.get('p1')['gateway_state'] == 'UNKNOWN'
        wire = args['spec']
        result = {'spec': wire, 'spec_digest': gateway_payload_digest(wire),
                  'execution_principal': 'execution', 'execution_actor_ref': 'exec-actor',
                  'state': 'ACTIVE', 'version': 1}
        return {'status': 'OBSERVED', 'schema_version': 1, 'operation': operation, 'result': result}
    monkeypatch.setattr(runtime, 'call', call)
    authority.accept(parent)
    assert authority.register('p1')['gateway_state'] == 'ACCEPTED'
    assert calls == [REGISTER]
    assert client.register_parent(parent, 'register1') is None
    assert calls == [REGISTER]
    store.close()


def test_sdk_delegate_uses_execution_journal_and_never_resends(tmp_path, monkeypatch):
    from decimal import Decimal
    from bt_api_base.gateway_delegation_protocol import GatewayDelegatedCommand
    from bt_api_base.gateway_direct_protocol import gateway_payload_digest
    from bt_api_base.market_protocol import MarketInstrumentKey
    from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope
    from bt_api_execution.gateway_delegate_host import ExecutionGatewayDelegateHost
    from bt_api_py._contracts.models import OrderRequest, OrderType, Side
    from bt_api_py.gateway_delegate_service_client import GatewayDelegateRoute, GatewayDelegateServiceClient
    from bt_api_py.service_runtime_thread import ThreadedServiceClientRuntime, BindingSnapshot, RuntimeSnapshot

    path = tmp_path / 'journal.sqlite'
    store, authority = open_parent(path, Owner())
    parent = spec(authority.scope)
    instrument = MarketInstrumentKey('ctp', 'SHFE', 'simnow', 'future', 'rb', 'rb')
    parent.update(instrument=instrument.to_dict(), quantity_unit='lots')
    authority.accept(parent)
    authority.register('p1')
    request = OrderRequest(symbol='rb', side=Side.SELL, order_type=OrderType.LIMIT, quantity=Decimal('3'),
                           account_id='a', client_order_id='cl1', price=Decimal('5'), idempotency_key='key1',
                           quantity_unit='lots', offset='close_today', exchange_id='SHFE')
    digest = gateway_payload_digest(request.to_dict())
    command = GatewayDelegatedCommand('a', 's', 'd', 'execution', 'exec-actor', 'execution', 'run',
                                      'key1', digest, instrument, 'g1', 'p1', 'child1')
    authority.prepare_child('p1', dict(child_id='child1', request_key='key1', trading_day='20261109',
                                       offset='close_today', quantity='3', payload_digest=digest))
    identity = WireIdentity(WireRole.SERVICE, 'execution', 'exec-actor')
    wire_scope = WireScope('d', account_ref='a', strategy_ref='s', run_id='run')
    route = GatewayDelegateRoute('upstream.direct_write', 'gateway', 'instance', 'scope', wire_scope, identity)
    runtime = ThreadedServiceClientRuntime(lambda: None)
    operations = frozenset({'direct.delegated_reserve', 'direct.delegated_dispatch'})
    binding = BindingSnapshot('registration', 'epoch', True, route.mount, route.service_id,
                              route.instance_id, route.scope_ref, wire_scope, operations, True)
    monkeypatch.setattr(runtime, 'status', lambda: RuntimeSnapshot('running', True, True, None, (binding,), 0, 0, identity, 'run'))
    host = ExecutionGatewayDelegateHost(authority)
    client = GatewayDelegateServiceClient(runtime, route=route, host=host)
    calls = []
    def call(_mount, operation, args, **_kwargs):
        calls.append(operation)
        if operation == 'direct.delegated_reserve':
            return {'status': 'OBSERVED', 'schema_version': 1, 'account_scope': 'a', 'strategy_scope': 's',
                    'actor_principal': 'execution', 'request_key': 'key1', 'reservation_id': 'reservation',
                    'digest': digest, 'state': 'RESERVED', 'version': 1,
                    'owner_generation': 'owner', 'reservation_generation': 'owner'}
        raise TimeoutError()
    monkeypatch.setattr(runtime, 'call', call)
    assert authority.dispatch_child('p1', 'child1', command, request, client).outcome == 'UNKNOWN'
    assert calls == ['direct.delegated_reserve', 'direct.delegated_dispatch']
    assert host.load_delegate_intent('key1').command == command
    assert host.load_delegate_dispatch_marker('key1') is not None
    with pytest.raises(InvalidStateTransition):
        authority.dispatch_child('p1', 'child1', command, request, client)
    store.close()
    store, authority = open_parent(path, Owner())
    host = ExecutionGatewayDelegateHost(authority)
    assert host.load_delegate_intent('key1').command == command
    assert host.load_delegate_dispatch_marker('key1') is not None
    store.close()


def test_selected_risk_reserves_before_gateway_and_child_remains_fenced(tmp_path):
    gateway, risk = Owner(), Owner()
    scope = ExecutionScope('FAKE', 'test', 'a', 's', '20261109')
    store = SqliteExecutionStore(tmp_path / 'journal.sqlite')
    lease = store.acquire_or_renew_lease(scope, 'writer', ttl_ns=300_000_000_000)
    authority = ParentAuthority(store, scope, lease, admission=lambda *_: True, gateway=gateway, risk=risk)
    parent = spec(scope)
    parent['risk_selected'] = True
    authority.accept(parent)
    assert authority.register('p1')['gateway_state'] == 'ACCEPTED'
    assert risk.calls == [('register', 'risk1')]
    assert gateway.calls == [('register', 'register1')]
    authority.prepare_child('p1', dict(child_id='c1', request_key='k1', trading_day='20261109',
                                       offset='close_today', quantity='3', payload_digest='b' * 64))
    with pytest.raises(InvalidStateTransition):
        authority.claim_child('p1', 'c1')
    store.close()


def test_execution_to_registry_gateway_parent_journal(tmp_path, monkeypatch):
    from decimal import Decimal
    from bt_api_base.direct_write_protocol import NativeOrderIdentity, PreDispatchRecord, WriteOperation
    from bt_api_base.direct_cancel_protocol import PreparedCancelRequest
    from bt_api_base.gateway_delegation_protocol import GatewayDelegatedCommand
    from bt_api_base.gateway_direct_protocol import gateway_payload_digest
    from bt_api_base.market_protocol import MarketInstrumentKey
    from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope
    from bt_api_gateway.direct_command_journal import GatewayDirectCommandJournal
    from bt_api_gateway.direct_delegation import GatewayDelegateGrant
    from bt_api_gateway.direct_cancel_dispatch import GatewayDirectCancelDispatcher
    from bt_api_gateway.direct_cancel_journal import GatewayDirectCancelJournal
    from bt_api_gateway.direct_dispatch import GatewayDirectDispatcher, NativeDispatchOutcome
    from bt_api_gateway.direct_parent_authority import GatewayParentAuthority
    from bt_api_gateway.session_guard import LocalAccountSessionGuard
    from bt_api_py._contracts.models import OrderRequest, OrderType, Side
    from bt_api_execution.gateway_delegate_host import ExecutionGatewayDelegateHost
    from bt_api_py.gateway_delegate_service_client import GatewayDelegateRoute, GatewayDelegateServiceClient
    from bt_api_py.gateway_delegation_service import GatewayDelegationService, GatewayDelegationSettings, OPERATIONS as DELEGATE_OPS
    from bt_api_py.gateway_delegate_actor_service import GatewayDelegateActorService, OPERATIONS as ACTOR_OPS
    from bt_api_py.gateway_direct_owner import GatewayDirectOwner, GatewayDirectOwnerResources
    from bt_api_py.gateway_parent_client import GatewayParentServiceClient
    from bt_api_py.gateway_parent_service import GatewayParentGrant, GatewayParentSettings, GatewayParentService, OPERATIONS
    from bt_api_py.service_registry import PrincipalGrant, ResourceGrant, ServiceCapability, ServiceRegistry
    from bt_api_py.service_runtime_thread import ThreadedServiceClientRuntime, BindingSnapshot, RuntimeSnapshot
    from tests.unit.test_service_registry import _next_request, _register

    directory = tmp_path / 'guard'
    directory.mkdir()
    parts = {}
    def factory():
        guard = LocalAccountSessionGuard(directory, 'a', 'owner')
        journal = GatewayDirectCommandJournal.create(tmp_path / 'gateway.sqlite', 'a', generation=guard.acquire())
        journal.enable_controls(guard)
        journal.enable_strategy_ledger(guard)
        journal.enable_account_extensions(guard)
        journal.enable_parent_authority(guard)
        parent = GatewayParentAuthority(journal, guard, now_ns=time.time_ns, trading_day=lambda: '20261109',
                                        transfer_completed=lambda _db, _spec: '0')
        def place(payload, port, account):
            native = NativeOrderIdentity('CTP', 'ref', {'trading_day': '20261109'}, 1)
            marker = port.commit(PreDispatchRecord(operation=WriteOperation.PLACE, host_scope=account,
                exchange_name='CTP', symbol='rb', identity=native, target_identity=None, request_summary=payload,
                trading_day='20261109', session_generation=1, created_at_utc='synthetic'))
            return NativeDispatchOutcome(True, native, marker)
        dispatcher = GatewayDirectDispatcher(journal, 'CTP', guard, place, lambda _: ('key',), lambda _: True)
        def cancel(target, symbol, port, account):
            prep = PreparedCancelRequest('provider-cancel', target, symbol, '20261109', 1, {'request_id': 1}, ('cancel-key',))
            marker = port.commit(PreDispatchRecord(WriteOperation.CANCEL, account, 'CTP', symbol, None,
                                                    target, {}, prep.trading_day, prep.session_generation, 'synthetic'))
            return NativeDispatchOutcome(True, target, marker)
        cancel_dispatcher = GatewayDirectCancelDispatcher(GatewayDirectCancelJournal(journal), dispatcher, guard, 'CTP', cancel, lambda _: True)
        parts.update(journal=journal, parent=parent)
        return GatewayDirectOwnerResources(dispatcher, journal, guard.release, cancel_dispatcher=cancel_dispatcher)
    owner = GatewayDirectOwner(factory, queue_size=2, max_queue_wait_seconds=2, busy_timeout_seconds=2)
    assert owner.start(2)
    registry = None
    store = None
    try:
        identity = WireIdentity(WireRole.SERVICE, 'execution', 'exec-actor')
        strategy = WireIdentity(WireRole.STRATEGY_CLIENT, 'strategy-principal', 's')
        wire_scope = WireScope('d', account_ref='a', strategy_ref='s', run_id='run')
        template = WireScope('d', account_ref='a', strategy_ref='s')
        strategy_ops = frozenset({'direct.reserve'})
        writes = frozenset({'direct.reserve', 'direct.parent.register', 'direct.parent.cancel', 'direct.delegate_actor_cancel', 'direct.delegate_actor_release'}) | DELEGATE_OPS
        execution_ops = OPERATIONS | DELEGATE_OPS | ACTOR_OPS
        cap = ServiceCapability('instance', 'service.direct_write', execution_ops | strategy_ops, writes)
        registry = ServiceRegistry(':memory:', service_id='gateway', capabilities=(cap,), grants={
            identity.principal_ref: (PrincipalGrant(identity, (ResourceGrant('instance', 'service.direct_write', 'scope', template, execution_ops),)),),
            strategy.principal_ref: (PrincipalGrant(strategy, (ResourceGrant('instance', 'service.direct_write', 'scope', template, strategy_ops),)),)},
            policy_version='p', requirements_version='r')
        instrument = MarketInstrumentKey('ctp', 'SHFE', 'simnow', 'future', 'rb', 'rb')
        grant = GatewayParentGrant(identity, 'a', 's', 'd', 'execution', 'g1', frozenset({instrument}), OPERATIONS)
        service = GatewayParentService(GatewayParentSettings((grant,)), owner=owner, journal=parts['journal'],
                                       authority=parts['parent'], instance_id='instance', scope_ref='scope')
        service.configure(registry)
        delegate_grant = GatewayDelegateGrant(identity, 'a', 's', 'd', 'execution', 'g1',
                                              frozenset({instrument}), frozenset({'direct.reserve', 'direct.dispatch',
                                                                                 'direct.command_status', 'direct.cancel', 'direct.cancel_status', 'direct.release'}))
        delegate_service = GatewayDelegationService(GatewayDelegationSettings((delegate_grant,),
            lambda value: value == instrument, parent_recheck=parts['parent'].recheck_child),
            owner=owner, journal=parts['journal'], instance_id='instance', scope_ref='scope')
        delegate_service.configure(registry)
        registry.register_handler('instance', 'direct.reserve', lambda *_: {})
        for op in OPERATIONS:
            registry.register_handler('instance', op, lambda ctx, args, op=op: service.handle(op, ctx, args))
        for op in DELEGATE_OPS:
            registry.register_handler('instance', op, lambda ctx, args, op=op: delegate_service.handle(op, ctx, args))
        actor_service = GatewayDelegateActorService(delegate_service,
            parent_cancel_recheck=parts['parent'].check_cancel_target,
            release_target_recheck=parts['parent'].check_release_target,
            parent_grant_resolver=lambda ctx, parent_operation: registry.gateway_parent_grant(
                instance_id='instance', scope_ref='scope', identity=ctx.identity,
                scope=ctx.scope, operation=parent_operation))
        for op in ACTOR_OPS:
            registry.register_handler('instance', op, lambda ctx, args, op=op: actor_service.handle(op, ctx, args))
        def register(actor, operations, key):
            return _register(registry, principal=actor.principal_ref, identity=actor, scope=wire_scope, key=key,
                             instance_id='instance', port='service.direct_write', scope_ref='scope', operations=tuple(operations))
        assert register(strategy, strategy_ops, 'strategy')['ok']
        registration = register(identity, execution_ops, 'execution')
        assert registration['ok'], registration
        issued = registration['result']
        route = GatewayDelegateRoute('upstream.direct_write', 'gateway', 'instance', 'scope', wire_scope, identity)
        runtime = ThreadedServiceClientRuntime(lambda: None)
        binding = BindingSnapshot(issued['registration_id'], issued['service_epoch'], True, route.mount,
                                  route.service_id, route.instance_id, route.scope_ref, wire_scope, execution_ops, True)
        monkeypatch.setattr(runtime, 'status', lambda: RuntimeSnapshot('running', True, True, None, (binding,), 0, 0, identity, 'run'))
        responses = []
        calls = []
        lose_release = [True]
        def rpc(_mount, op, arguments, **_kwargs):
            calls.append(op)
            response = registry.handle(identity.principal_ref, _next_request(op, {
                'registration_id': issued['registration_id'], 'service_epoch': issued['service_epoch'],
                'instance_id': 'instance', 'arguments': arguments}, identity=identity, scope=wire_scope))
            assert response['ok'], response
            responses.append(response)
            if op == 'direct.delegate_actor_release' and lose_release[0]:
                lose_release[0] = False
                raise TimeoutError('synthetic lost release reply')
            return response['result']
        monkeypatch.setattr(runtime, 'call', rpc)
        store, authority = open_parent(tmp_path / 'execution.sqlite', Owner())
        authority.gateway = None
        client = GatewayParentServiceClient(runtime, route=route, host=authority)
        authority.bind_gateway(client)
        parent = spec(authority.scope)
        parent.update(offset='open', target_position_ref=None, quantity_unit='lots')
        authority.accept(parent)
        assert authority.register('p1')['gateway_state'] == 'ACCEPTED', responses
        request = OrderRequest(symbol='rb', side=Side.SELL, order_type=OrderType.LIMIT, quantity=Decimal('3'),
                               account_id='a', client_order_id='child', price=Decimal('5'), idempotency_key='key1',
                               quantity_unit='lots', offset='open', exchange_id='SHFE')
        digest = gateway_payload_digest(request.to_dict())
        command = GatewayDelegatedCommand('a', 's', 'd', 'execution', 'exec-actor', 'execution', 'run',
                                          'key1', digest, instrument, 'g1', 'p1', 'child1')
        authority.prepare_child('p1', dict(child_id='child1', request_key='key1', trading_day='20261109',
                                          offset='open', quantity='3', payload_digest=digest))
        delegate_client = GatewayDelegateServiceClient(runtime, route=route, host=ExecutionGatewayDelegateHost(authority))
        assert authority.dispatch_child('p1', 'child1', command, request, delegate_client).outcome == 'QUEUED'
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            status = delegate_client.actor_status('key1')
            if status.outcome == 'OBSERVED' and status.observation.command.state == 'SENT':
                break
            time.sleep(.01)
        assert status.outcome == 'OBSERVED' and status.observation.command.state == 'SENT'
        request2 = OrderRequest(symbol='rb', side=Side.SELL, order_type=OrderType.LIMIT, quantity=Decimal('2'),
                                account_id='a', client_order_id='child2', price=Decimal('5'), idempotency_key='key2',
                                quantity_unit='lots', offset='open', exchange_id='SHFE')
        digest2 = gateway_payload_digest(request2.to_dict())
        command2 = GatewayDelegatedCommand('a', 's', 'd', 'execution', 'exec-actor', 'execution', 'run',
                                           'key2', digest2, instrument, 'g1', 'p1', 'child2')
        authority.prepare_child('p1', dict(child_id='child2', request_key='key2', trading_day='20261109',
                                          offset='open', quantity='2', payload_digest=digest2))
        authority.claim_child('p1', 'child2')
        assert delegate_client.reserve(command2, request2).outcome == 'OBSERVED'
        assert authority.cancel('p1', 'cancel1', 1)['cancel_state'] == 'REQUESTED'
        results = authority.advance_cancel_children('p1', delegate_client)
        assert len(results) == 2 and results[0].outcome in {'QUEUED', 'OBSERVED'} and results[1].outcome == 'UNKNOWN', (results, responses[-1])
        with store._read_transaction() as cursor:
            child = cursor.execute("SELECT * FROM execution_parent_children WHERE child_id='child1'").fetchone()
            frozen = cursor.execute("SELECT * FROM execution_parent_child_cancels WHERE child_id='child1'").fetchone()
            assert child['state'] == 'UNKNOWN'
            assert frozen['intent_json'] is not None and frozen['dispatch_marker_json'] is not None
            released = cursor.execute("SELECT * FROM execution_parent_children WHERE child_id='child2'").fetchone()
            assert released['state'] == 'UNKNOWN'
        recovered = authority.advance_cancel_children('p1', delegate_client)
        assert recovered[1].outcome == 'OBSERVED'
        assert calls.count('direct.delegate_actor_release') == 1
        assert calls.count('direct.delegate_actor_release_status') == 1
        with store._read_transaction() as cursor:
            released = cursor.execute("SELECT * FROM execution_parent_children WHERE child_id='child2'").fetchone()
            assert released['state'] == 'TERMINAL' and released['filled_quantity'] == '0'
        with sqlite3.connect(tmp_path / 'gateway.sqlite') as db:
            assert db.execute('SELECT state FROM direct_execution_parents WHERE parent_ref=?', ('p1',)).fetchone()[0] == 'CANCEL_REQUESTED'
    finally:
        if store is not None:
            store.close()
        if registry is not None:
            registry.close()
        assert owner.close(2)


def test_cancel_lost_reply_is_reconciled_with_original_id_and_never_resent(tmp_path):
    class LostCancel(Owner):
        def cancel_parent(self, spec, operation_id, expected_version):
            self.calls.append(('cancel', operation_id))
            raise TimeoutError()
        def query_operation(self, spec, operation_id):
            self.calls.append(('query', operation_id))
            return {'state': 'ACCEPTED', 'result': {'targets': []}}
    owner = LostCancel()
    path = tmp_path / 'journal.sqlite'
    store, authority = open_parent(path, owner)
    authority.accept(spec(authority.scope))
    authority.register('p1')
    assert authority.cancel('p1', 'cancel1', 1)['cancel_state'] == 'UNKNOWN'
    assert authority.cancel('p1', 'cancel1', 1)['cancel_state'] == 'UNKNOWN'
    store.close()
    store, authority = open_parent(path, owner)
    assert authority.reconcile_cancel('p1')['cancel_state'] == 'REQUESTED'
    assert owner.calls == [('register', 'register1'), ('cancel', 'cancel1'), ('query', 'cancel1')]
    store.close()


def test_explicit_service_config_constructs_zmq_session_without_connecting(tmp_path):
    from bt_api_base.service_protocol import WireIdentity, WireRole, WireScope
    from bt_api_execution.gateway_session import ExecutionGatewaySession
    from bt_api_py.gateway_delegate_service_client import GatewayDelegateRoute
    from bt_api_py.service_config import parse_service_config
    from bt_api_py.service_runtime import ClientCurveKeys
    from tests.unit.test_service_config import _service_config, _read_profile

    raw = _service_config()
    raw['clients'] = {'gateway': {'config_schema_version': 1, 'settings': {'bindings': [{
        'mount': 'upstream.direct_write', 'account_ref': 'a', 'service_id': 'gateway',
        'instance_id': 'instance', 'scope_ref': 'scope', 'transport_ref': 'gateway-rpc', 'required': True}]}}}
    raw['communication']['profiles']['gateway-rpc'] = _read_profile('rpc_connect', 'tcp://127.0.0.1:7855')
    config = parse_service_config(raw)
    identity = WireIdentity(WireRole.SERVICE, 'execution', 'exec-actor')
    peer = WireIdentity(WireRole.SERVICE, 'gateway', 'gateway-actor')
    route = GatewayDelegateRoute('upstream.direct_write', 'gateway', 'instance', 'scope',
                                 WireScope('d', account_ref='a', strategy_ref='s', run_id='run'), identity)
    scope = ExecutionScope('FAKE', 'test', 'a', 's', '20261109')
    store = SqliteExecutionStore(tmp_path / 'journal.sqlite')
    lease = store.acquire_or_renew_lease(scope, 'writer', ttl_ns=300_000_000_000)
    session = ExecutionGatewaySession(config=config, route=route, expected_gateway=peer,
        security_profiles={'test-security': ClientCurveKeys(b'server', b'public', b'secret')},
        store=store, scope=scope, lease=lease, admission=lambda *_: True)
    assert session.runtime.status().state == 'new'
    assert session.parent.gateway is session.parent_client
    assert session.delegate_host.authority is session.parent
    store.close()


def test_selected_risk_unavailable_keeps_durable_parent_and_never_registers_gateway(tmp_path):
    gateway = Owner()
    store, authority = open_parent(tmp_path / 'journal.sqlite', gateway)
    parent = spec(authority.scope)
    parent['risk_selected'] = True
    assert authority.accept(parent)['risk_state'] == 'PENDING'
    assert authority.register('p1')['gateway_state'] == 'PENDING'
    assert gateway.calls == []
    store.close()
