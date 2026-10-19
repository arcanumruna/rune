"""Authentication in the dr-e2e harness (Task 20): the key, the bootstrap, the client credentials (no Docker)."""
import os

import pytest

import harness.auth as ha
from harness.dockerutil import CmdResult


@pytest.fixture(autouse=True)
def _fresh_state(monkeypatch):
    monkeypatch.setattr(ha, "_STATE", None)
    monkeypatch.setattr(ha, "_BOOTSTRAPPED", set())
    monkeypatch.setenv("DR_QL_IMAGE", "rune-ql-probe:test")
    monkeypatch.setenv("DR_IMAGES_PREPARED", "1")   # ensure_ql_image is a no-op
    yield


def _res(rc=0, out="", err=""):
    return CmdResult(args=[], returncode=rc, stdout=out, stderr=err) if "args" in CmdResult.__dataclass_fields__ \
        else CmdResult(returncode=rc, stdout=out, stderr=err)


def test_state_has_private_one_time_password_admin_and_e2e_passwords():
    st = ha.state()
    for name in ("otp", "admin.pw", "e2e.pw"):
        path = os.path.join(st.work_dir, name)
        assert oct(os.stat(path).st_mode & 0o777) == "0o600"
        assert len(open(path).read().strip()) >= 12            # the CLI's minimum password length
    assert ha.state() is st                                     # one directory per process


def test_client_containers_log_in_as_the_e2e_user_with_a_password_file_never_a_password():
    args = ha.docker_args()
    assert "RUNE_USER=e2e" in args and "RUNE_PASSWORD_FILE=/work/e2e.pw" in args
    assert not any("PASSWORD=" in a and "FILE" not in a for a in args)
    assert any(a.endswith(":/work:ro") for a in args)


def test_ensure_key_exports_the_secrets_dir_and_uses_gen_cluster_key(monkeypatch):
    calls = []

    def fake_run(args, **kw):
        calls.append(args)
        out_dir = args[args.index("-v") + 1].split(":")[0]
        open(os.path.join(out_dir, "cluster.key"), "w").write("ab" * 32)
        return _res()

    monkeypatch.setattr(ha, "run", fake_run)
    monkeypatch.delenv("RUNE_SECRETS_DIR", raising=False)
    key = ha.ensure_key()
    assert key.endswith("secrets/cluster.key") and os.environ["RUNE_SECRETS_DIR"] == ha.state().secrets_dir
    assert calls[0][-4:] == ["security", "gen-cluster-key", "--out", "/out/cluster.key"]
    assert ha.ensure_key() == key and len(calls) == 1          # once per process


def test_bootstrap_runs_bootstrap_then_alter_then_the_role_and_user_and_is_idempotent_per_network(monkeypatch):
    seen = []

    def fake_run(args, **kw):
        seen.append(args)
        return _res()

    monkeypatch.setattr(ha, "run", fake_run)
    ha.bootstrap("net1", "rune-1:40200")
    verbs = [" ".join(a[a.index("rune"):][:3]) for a in seen]
    assert verbs[0].startswith("rune security bootstrap")
    assert verbs[1].startswith("rune ql --user") and "/work/otp" in seen[1] and "alter-admin.rql" in " ".join(seen[1])
    assert "/work/admin.pw" in seen[2] and "create-e2e-user.rql" in " ".join(seen[2])
    n = len(seen)
    ha.bootstrap("net1", "rune-1:40200")                       # same cluster: nothing more
    assert len(seen) == n
    ha.forget("net1")
    ha.bootstrap("net1", "rune-1:40200")                       # a fresh cluster under the same name: again
    assert len(seen) == 2 * n
    # the password literals live in files that are gone again
    assert sorted(os.listdir(ha.state().work_dir)) == ["admin.pw", "e2e.pw", "otp"]


def test_bootstrap_retries_while_the_cluster_forms_but_never_when_already_bootstrapped(monkeypatch):
    outcomes = [_res(1, err="rune error: connect timed out"), _res(1, err="NotLeader"), _res()]
    state = {"i": 0}

    def fake_run(args, **kw):
        if "bootstrap" in args and state["i"] < len(outcomes):
            state["i"] += 1
            return outcomes[state["i"] - 1]
        return _res()

    monkeypatch.setattr(ha, "run", fake_run)
    monkeypatch.setattr(ha.time, "sleep", lambda s: None)
    ha.bootstrap("net2", "rune-1:40200")
    assert state["i"] == 3

    monkeypatch.setattr(ha, "run", lambda args, **kw: _res(1, err="rune: bootstrap refused: this cluster is already bootstrapped"))
    with pytest.raises(RuntimeError, match="already bootstrapped"):
        ha.bootstrap("net3", "rune-1:40200")


def test_bootstrap_failure_that_is_not_retriable_raises_with_the_cli_output(monkeypatch):
    monkeypatch.setattr(ha, "run", lambda args, **kw: _res(1, err="rune: bootstrap refused: code 3 bad key"))
    with pytest.raises(RuntimeError, match="bad key"):
        ha.bootstrap("net4", "rune-1:40200")
