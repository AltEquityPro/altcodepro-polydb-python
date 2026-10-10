"""Inside a managed-identity host the managed identity is tried first, so a stray AZURE_CLIENT_SECRET cannot break Key Vault."""
import sys
import types
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def fake_identity(monkeypatch):
    mod = types.ModuleType("azure.identity")
    mod.ChainedTokenCredential = MagicMock(name="Chained")
    mod.DefaultAzureCredential = MagicMock(name="Default")
    mod.ManagedIdentityCredential = MagicMock(name="MI")
    monkeypatch.setitem(sys.modules, "azure.identity", mod)
    return mod


def test_a_managed_identity_host_puts_the_managed_identity_first(fake_identity, monkeypatch):
    from polydb.adapters.AzureKeyVaultAdapter import AzureKeyVaultAdapter
    monkeypatch.setenv("IDENTITY_ENDPOINT", "http://169.254.0.1/msi")
    monkeypatch.setenv("AZURE_CLIENT_ID", "mi-client-id")
    AzureKeyVaultAdapter._build_credential()
    fake_identity.ManagedIdentityCredential.assert_called_once_with(client_id="mi-client-id")
    chain_args = fake_identity.ChainedTokenCredential.call_args.args
    assert chain_args[0] is fake_identity.ManagedIdentityCredential.return_value and chain_args[1] is fake_identity.DefaultAzureCredential.return_value


def test_elsewhere_the_default_chain_is_unchanged(fake_identity, monkeypatch):
    from polydb.adapters.AzureKeyVaultAdapter import AzureKeyVaultAdapter
    monkeypatch.delenv("IDENTITY_ENDPOINT", raising=False)
    monkeypatch.delenv("MSI_ENDPOINT", raising=False)
    assert AzureKeyVaultAdapter._build_credential() is fake_identity.DefaultAzureCredential.return_value
    fake_identity.ManagedIdentityCredential.assert_not_called()
