"""Read-only verification of process-local BasicAdmission claim provenance."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
import test_execution_basic_admission as admission_fixtures

from bt_api_base.service_protocol import WireIdentity, WireRole
from bt_api_execution import (
    ExecutionAdmissionContextV1,
    ExecutionAdmissionValidatorRegistry,
    ExecutionBasicAdmissionGate,
    ExecutionScope,
    SqliteExecutionStore,
    WriterLeaseUnavailable,
)
from bt_api_execution.basic_admission import ExecutionBasicAdmissionDenied
from bt_api_execution.contracts import order_intent_from_payload
from bt_api_execution.service_order_entry import SUBMIT_OPERATION


def _intent(intent_id: str = "exe04-claimed-provenance"):
    arguments = admission_fixtures._arguments(intent_id)
    arguments["scope"] = admission_fixtures._EXECUTION_SCOPE.to_payload()
    return order_intent_from_payload(arguments)


def _context(
    *,
    identity: WireIdentity = admission_fixtures._IDENTITY,
    wire_scope=admission_fixtures._WIRE_SCOPE,
    execution_scope: ExecutionScope = admission_fixtures._EXECUTION_SCOPE,
):
    return ExecutionAdmissionContextV1(
        identity.principal_ref,
        identity,
        wire_scope,
        execution_scope,
        SUBMIT_OPERATION,
    )


@contextmanager
def _case(
    path,
    *,
    permit_ttl_ns: int = 5_000_000_000,
    max_active_permits: int = 8,
):
    store = SqliteExecutionStore(path)
    provider = admission_fixtures._AuthorizedProvider()
    validator = admission_fixtures._validator(provider)
    validators = ExecutionAdmissionValidatorRegistry((validator,))
    clock = admission_fixtures._Clock()
    gate = ExecutionBasicAdmissionGate(
        store,
        validators,
        permit_ttl_ns=permit_ttl_ns,
        clock_ns=clock,
        max_active_permits=max_active_permits,
        retain_claimed_authorizations=True,
    )
    lease = store.acquire_or_renew_lease(
        admission_fixtures._EXECUTION_SCOPE, "exe04-provenance-writer"
    )
    case = SimpleNamespace(
        store=store,
        provider=provider,
        validator=validator,
        validators=validators,
        clock=clock,
        gate=gate,
        lease=lease,
        context=_context(),
        intent=_intent(),
    )
    try:
        yield case
    finally:
        store.close()


def _reserve_and_claim(case, *, intent=None, context=None):
    intent = case.intent if intent is None else intent
    context = case.context if context is None else context
    reference = case.gate.reserve_for_dispatch(
        context, intent, case.provider, case.lease
    )
    authorization = case.gate.claim_for_dispatch(
        reference, intent, context, case.provider, case.lease
    )
    return reference, authorization


@pytest.mark.unit
def test_claimed_authorization_verifies_same_object_repeatedly_without_writes(tmp_path):
    with _case(tmp_path / "claimed-provenance.sqlite3") as case:
        _reference, authorization = _reserve_and_claim(case)
        record_before = case.store.get(case.intent.intent_id, scope=case.intent.scope)
        changes_before = case.store._connection.total_changes

        first = case.gate.verify_claimed_authorization(
            authorization, case.intent, case.provider, case.lease
        )
        second = case.gate.verify_claimed_authorization(
            authorization, case.intent, case.provider, case.lease
        )

        assert first is authorization
        assert second is authorization
        assert case.store._connection.total_changes == changes_before
        assert case.store.get(case.intent.intent_id, scope=case.intent.scope) == record_before
        assert case.provider.submitted == []
        assert case.provider.unauthorized_calls == []

        default_gate = ExecutionBasicAdmissionGate(
            case.store, case.validators, clock_ns=case.clock
        )
        with pytest.raises(ExecutionBasicAdmissionDenied):
            default_gate.verify_claimed_authorization(
                authorization, case.intent, case.provider, case.lease
            )

        copied = replace(authorization)
        assert copied == authorization and copied is not authorization
        with pytest.raises(ExecutionBasicAdmissionDenied):
            case.gate.verify_claimed_authorization(
                copied, case.intent, case.provider, case.lease
            )

        restarted_gate = ExecutionBasicAdmissionGate(
            case.store,
            case.validators,
            clock_ns=case.clock,
            retain_claimed_authorizations=True,
        )
        with pytest.raises(ExecutionBasicAdmissionDenied):
            restarted_gate.verify_claimed_authorization(
                authorization, case.intent, case.provider, case.lease
            )
        assert case.gate.verify_claimed_authorization(
            authorization, case.intent, case.provider, case.lease
        ) is authorization


@pytest.mark.unit
def test_claimed_authorization_rechecks_intent_port_and_current_policy(tmp_path):
    with _case(tmp_path / "claimed-bindings.sqlite3") as case:
        _reference, authorization = _reserve_and_claim(case)

        wrong_intent = _intent("exe04-other-intent")
        with pytest.raises(ExecutionBasicAdmissionDenied):
            case.gate.verify_claimed_authorization(
                authorization, wrong_intent, case.provider, case.lease
            )

        other_provider = admission_fixtures._AuthorizedProvider()
        with pytest.raises(ExecutionBasicAdmissionDenied):
            case.gate.verify_claimed_authorization(
                authorization, case.intent, other_provider, case.lease
            )

        # A policy-registry replacement models a trusted composition update;
        # callers cannot mutate the immutable registry themselves.
        changed_validator = admission_fixtures._validator(
            case.provider, version="v2"
        )
        case.gate._validators = ExecutionAdmissionValidatorRegistry(
            (changed_validator,)
        )
        with pytest.raises(ExecutionBasicAdmissionDenied):
            case.gate.verify_claimed_authorization(
                authorization, case.intent, case.provider, case.lease
            )

        assert case.provider.submitted == []
        assert other_provider.submitted == []


@pytest.mark.unit
def test_claimed_authorization_rechecks_expiry_and_writer_fencing(tmp_path):
    with _case(
        tmp_path / "claimed-expiry.sqlite3", permit_ttl_ns=1_000_000
    ) as case:
        _reference, authorization = _reserve_and_claim(case)
        case.clock.now_ns = authorization.expires_at_ns
        with pytest.raises(ExecutionBasicAdmissionDenied):
            case.gate.verify_claimed_authorization(
                authorization, case.intent, case.provider, case.lease
            )

    with _case(tmp_path / "claimed-fence.sqlite3") as case:
        _reference, authorization = _reserve_and_claim(case)
        released = case.store.release_lease(
            case.intent.scope,
            case.lease.owner_id,
            fencing_token=case.lease.fencing_token,
        )
        assert released is True
        replacement_lease = case.store.acquire_or_renew_lease(
            case.intent.scope, "exe04-replacement-writer"
        )
        assert replacement_lease.fencing_token > case.lease.fencing_token
        changes_before_verify = case.store._connection.total_changes

        with pytest.raises(WriterLeaseUnavailable):
            case.gate.verify_claimed_authorization(
                authorization, case.intent, case.provider, case.lease
            )
        with pytest.raises(ExecutionBasicAdmissionDenied):
            case.gate.verify_claimed_authorization(
                authorization, case.intent, case.provider, replacement_lease
            )
        assert case.store._connection.total_changes == changes_before_verify
        assert case.provider.submitted == []


@pytest.mark.unit
def test_claim_capacity_covers_pending_in_progress_and_claimed_until_ttl(tmp_path, monkeypatch):
    with _case(
        tmp_path / "claim-capacity.sqlite3", max_active_permits=1
    ) as case:
        reference = case.gate.reserve_for_dispatch(
            case.context, case.intent, case.provider, case.lease
        )

        validation_entered = threading.Event()
        continue_validation = threading.Event()
        original_validate = case.validators.validate

        def block_claim_validation(context, intent, dispatch_port, *, now_ns):
            if threading.current_thread().name == "basic-admission-claim":
                validation_entered.set()
                if not continue_validation.wait(timeout=2.0):
                    raise TimeoutError("claim validation test gate timed out")
            return original_validate(context, intent, dispatch_port, now_ns=now_ns)

        monkeypatch.setattr(case.validators, "validate", block_claim_validation)
        claim_results = []
        claim_errors = []

        def claim_once():
            try:
                claim_results.append(
                    case.gate.claim_for_dispatch(
                        reference,
                        case.intent,
                        case.context,
                        case.provider,
                        case.lease,
                    )
                )
            except BaseException as exc:  # recorded for bounded worker cleanup
                claim_errors.append(exc)

        claim_thread = threading.Thread(
            target=claim_once, name="basic-admission-claim"
        )
        claim_thread.start()
        reserve_errors = []
        reserve_done = threading.Event()

        def reserve_while_claiming():
            try:
                case.gate.reserve_for_dispatch(
                    case.context, case.intent, case.provider, case.lease
                )
            except BaseException as exc:  # capacity denial is returned to the test thread
                reserve_errors.append(exc)
            finally:
                reserve_done.set()

        reserve_thread = None
        try:
            assert validation_entered.wait(timeout=1.0)
            reserve_thread = threading.Thread(
                target=reserve_while_claiming, name="basic-admission-reserve"
            )
            reserve_thread.start()
            assert reserve_done.wait(timeout=1.0), "Gate lock was held across validator work"
            assert len(reserve_errors) == 1
            assert isinstance(reserve_errors[0], ExecutionBasicAdmissionDenied)
        finally:
            continue_validation.set()
            claim_thread.join(timeout=2.0)
            if reserve_thread is not None:
                reserve_thread.join(timeout=2.0)
        assert not claim_thread.is_alive()
        assert reserve_thread is None or not reserve_thread.is_alive()
        assert claim_errors == []
        assert len(claim_results) == 1
        authorization = claim_results[0]

        with pytest.raises(ExecutionBasicAdmissionDenied):
            case.gate.reserve_for_dispatch(
                case.context, case.intent, case.provider, case.lease
            )
        case.clock.now_ns = authorization.expires_at_ns
        renewed_reference = case.gate.reserve_for_dispatch(
            case.context, case.intent, case.provider, case.lease
        )

        wrong_identity = WireIdentity(
            WireRole.STRATEGY_CLIENT, "exe04-other-principal", "strategy-a"
        )
        wrong_context = _context(identity=wrong_identity)
        with pytest.raises(ExecutionBasicAdmissionDenied):
            case.gate.claim_for_dispatch(
                renewed_reference,
                case.intent,
                wrong_context,
                case.provider,
                case.lease,
            )

        # A failed claim releases its in-progress capacity reservation.
        next_reference = case.gate.reserve_for_dispatch(
            case.context, case.intent, case.provider, case.lease
        )
        assert type(next_reference) is str and next_reference != renewed_reference
