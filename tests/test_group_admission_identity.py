"""Identity checks for account-scoped group admission composition."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from bt_api_execution import AccountScopedGroupAdmissionAdapter
from bt_api_execution.errors import ContractValidationError


class _GroupAdmissionGate:
    def __init__(self) -> None:
        self.group_claims = 0

    def claim_group_leg(self, *_args: object, **_kwargs: object) -> None:
        self.group_claims += 1


def test_account_scoped_adapter_requires_exact_gate_identity() -> None:
    account_scope = SimpleNamespace(key="risk-scope/same-account/demo")
    first_gate = _GroupAdmissionGate()
    second_gate = _GroupAdmissionGate()
    first_adapter = AccountScopedGroupAdmissionAdapter(first_gate, account_scope=account_scope)
    second_adapter = AccountScopedGroupAdmissionAdapter(second_gate, account_scope=account_scope)

    assert first_adapter.risk_scope_fingerprint == second_adapter.risk_scope_fingerprint
    assert first_adapter.require_admission_gate(first_gate) is None
    assert second_adapter.require_admission_gate(second_gate) is None

    with pytest.raises(ContractValidationError) as first_mismatch:
        first_adapter.require_admission_gate(second_gate)
    with pytest.raises(ContractValidationError) as second_mismatch:
        second_adapter.require_admission_gate(first_gate)

    assert first_mismatch.value.code == "invalid_execution_contract"
    assert second_mismatch.value.code == "invalid_execution_contract"
    assert first_gate.group_claims == 0
    assert second_gate.group_claims == 0
