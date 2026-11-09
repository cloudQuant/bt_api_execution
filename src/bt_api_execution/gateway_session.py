"""Explicit, ZMQ-backed Execution service composition for a single strategy scope.

Configuration and authenticated identity/key material are supplied by trusted
application wiring; no credentials or policies are loaded by Execution.
"""
from __future__ import annotations

from dataclasses import asdict

from .gateway_delegate_host import ExecutionGatewayDelegateHost
from .parent_authority import ParentAuthority


class ExecutionGatewaySession:
    """Own the SDK client runtime; the caller retains ownership of journal/lease."""
    def __init__(self, *, config, route, expected_gateway, security_profiles,
                 store, scope, lease, admission, risk=None, queue_size=32):
        from bt_api_base.service_protocol import WireIdentity, WireRole
        from bt_api_py.gateway_delegate_service_client import GatewayDelegateRoute, GatewayDelegateServiceClient
        from bt_api_py.gateway_parent_client import GatewayParentServiceClient
        from bt_api_py.gateway_parent_service import REGISTER, QUERY, CANCEL, OPERATION
        from bt_api_py.service_config import ServiceConfig
        from bt_api_py.service_runtime import ClientCurveKeys, ServiceClientRuntime, ZmqTransportFactory
        from bt_api_py.service_runtime_thread import ThreadedServiceClientRuntime

        if (type(config) is not ServiceConfig or type(route) is not GatewayDelegateRoute or
                type(expected_gateway) is not WireIdentity or expected_gateway.role is not WireRole.SERVICE or
                type(security_profiles) is not dict or not security_profiles or
                any(type(k) is not str or not k or type(v) is not ClientCurveKeys for k, v in security_profiles.items())):
            raise ValueError('INVALID_EXECUTION_GATEWAY_COMPOSITION')
        if (route.identity.role is not WireRole.SERVICE or not callable(admission) or
                admission('bind_scope', {'execution_scope': scope.to_payload(),
                                         'wire_scope': asdict(route.scope),
                                         'identity': asdict(route.identity)}) is not True):
            raise ValueError('EXECUTION_GATEWAY_SCOPE_MISMATCH')
        # Service configuration is parsed by the SDK and cannot supply Python
        # factories, principals, or keys. The explicit ZMQ profile is supplied
        # from the trusted composition root.
        profiles = dict(security_profiles)
        operations = frozenset({REGISTER, QUERY, CANCEL, OPERATION,
                                'direct.delegated_reserve', 'direct.delegated_dispatch',
                                'direct.delegate_actor_status', 'direct.delegate_actor_cancel',
                                'direct.delegate_actor_cancel_status', 'direct.delegate_actor_release',
                                'direct.delegate_actor_release_status'})
        def scope_resolver(ref):
            if ref != route.scope_ref:
                raise ValueError('INVALID_SCOPE')
            return route.scope
        def peer_resolver(service_id):
            if service_id != route.service_id:
                raise ValueError('INVALID_PEER')
            return expected_gateway
        def runtime_factory():
            return ServiceClientRuntime(config, identity=route.identity,
                scope_resolver=scope_resolver, peer_resolver=peer_resolver,
                transport_factory=ZmqTransportFactory(profiles), run_id=route.scope.run_id,
                requested_operations={route.mount: operations})

        self.runtime = ThreadedServiceClientRuntime(runtime_factory, queue_size=queue_size)
        self.parent = ParentAuthority(store, scope, lease, admission=admission, risk=risk)
        self.delegate_host = ExecutionGatewayDelegateHost(self.parent)
        self.parent_client = GatewayParentServiceClient(self.runtime, route=route, host=self.parent)
        self.delegate_client = GatewayDelegateServiceClient(self.runtime, route=route, host=self.delegate_host)
        self.parent.bind_gateway(self.parent_client)
        self._operations = operations
        self._started = False

    def start(self, *, timeout_seconds=5.0):
        if self._started:
            raise RuntimeError('EXECUTION_GATEWAY_ALREADY_STARTED')
        snapshot = self.runtime.start(timeout_seconds=timeout_seconds)
        try:
            for operation in self._operations:
                self.parent_client._registration(operation)
        except Exception:
            self.runtime.close(timeout_seconds=timeout_seconds)
            raise RuntimeError('EXECUTION_GATEWAY_BINDING_UNAVAILABLE') from None
        self._started = True
        return snapshot

    def close(self, *, timeout_seconds=5.0):
        self._started = False
        return self.runtime.close(timeout_seconds=timeout_seconds)
