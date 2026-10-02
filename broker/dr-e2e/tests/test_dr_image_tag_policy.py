"""DR client image tag policy (regression): the harness must run a UNIQUE,
git-SHA-tagged rune-dr-client image and REFUSE the generic :dev / :latest tags
or an unset DR_CLIENT_IMAGE.  A stale :dev image silently exercised the old
(pre-F8c, futures-based) client and reported green for code not under test
(rune-client-cpp F8c C9 review, finding 2).  Pure: no Docker needed."""
import pytest

from harness.driver import dr_image


def test_unset_image_is_refused_regression(monkeypatch):
    monkeypatch.delenv("DR_CLIENT_IMAGE", raising=False)
    with pytest.raises(RuntimeError, match="DR_CLIENT_IMAGE"):
        dr_image()


@pytest.mark.parametrize("img", ["rune-dr-client:dev", "rune-dr-client:latest", "rune-dr-client",
                                 "rune-dr-client:", "rune-dr-client:mngreen", "rune-dr-client:c9-wip"])
def test_non_sha_tags_are_refused_regression(monkeypatch, img):
    monkeypatch.setenv("DR_CLIENT_IMAGE", img)
    with pytest.raises(RuntimeError, match="git SHA"):
        dr_image()


@pytest.mark.parametrize("img", ["rune-dr-client:5aaff86", "rune-dr-client:424077a1b2c3",
                                 "registry.local:5000/rune-dr-client:5aaff86"])
def test_sha_tag_is_accepted_regression(monkeypatch, img):
    monkeypatch.setenv("DR_CLIENT_IMAGE", img)
    assert dr_image() == img
