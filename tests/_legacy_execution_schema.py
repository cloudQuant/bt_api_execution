"""Helpers for building precise pre-v24 execution-store fixtures."""


def drop_v24_callback_owner_scope_schema(connection) -> None:
    """Remove only the owner-scope objects introduced by schema version 24."""
    for trigger in (
        "ctp_callback_session_owner_scopes_owner_match",
        "ctp_callback_session_owner_scopes_immutable_update",
        "ctp_callback_session_owner_scopes_immutable_delete",
    ):
        connection.execute(f'DROP TRIGGER IF EXISTS "{trigger}"')
    connection.execute(
        'DROP TABLE IF EXISTS "ctp_dispatch_callback_session_owner_scopes"'
    )
