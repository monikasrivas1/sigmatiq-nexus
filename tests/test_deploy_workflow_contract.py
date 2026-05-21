from pathlib import Path


WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "deploy-nexus-prod.yml"


def test_prod_deploy_starts_db_signal_worker_container_app():
    workflow = WORKFLOW.read_text()

    assert "KEY_VAULT_NAME: kvsigmatiqprodiotgrj" in workflow
    assert "DB_SIGNAL_APP_NAME: sigmatiq-nexus-db-signal-ca" in workflow
    assert 'az keyvault secret show --vault-name "$KEY_VAULT_NAME" --name shared-database-url' in workflow
    assert 'test -n "$SHARED_DB_URL"' in workflow
    assert "--name \"$DB_SIGNAL_APP_NAME\"" in workflow
    assert '--command "nexus-db-signal-worker"' in workflow
    assert '"SHARED_DATABASE_URL=secretref:shared-db-url"' in workflow
    assert '"NEXUS_DB_SIGNAL_SYMBOLS=SPY,QQQ,IWM,UVXY"' in workflow
    assert '"NEXUS_DB_SIGNAL_POLL_SECONDS=30"' in workflow
    assert '"NEXUS_DB_SIGNAL_MAX_WINDOW_LAG_SECONDS=300"' in workflow
    assert '"NEXUS_DB_REPLAY_INCLUDE_RELAXED_WATCH=true"' in workflow
