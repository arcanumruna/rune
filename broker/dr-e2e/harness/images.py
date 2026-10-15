"""Session-wide image preparation: every image the suite runs is made ready ONCE, before any test.

    prepare_all()   broker image (built with DR_E2E_BUILD=1, else it must already exist),
                    dr client image (built from the client checkout, sha-labelled),
                    rune CLI (ql probe) image, netem sidecar image, alpine (volume helper).

Called by conftest's pytest_sessionstart (the xdist controller, or the single process), outside every
per-test timeout.  A failure raises ImagePreparationError and conftest ends the session with it:
a broken build is never discovered by - or charged to - an individual test.  The per-call ensure
functions (Cluster.up -> ensure_dr_client_image, create_topics -> the CLI image) are then no-ops in
this process and in every xdist worker (DR_IMAGES_PREPARED=1 is inherited); run without the
session hook (e.g. a script importing the harness) they still build lazily, once per process.

The "dirty client tree must never run on a stale image" rule is unchanged: the client and CLI
images are (re)built from the working tree at session start, and the client image's sha label is
still checked.
"""

from __future__ import annotations

import os

from . import timing
from .dockerutil import _git_sha, run

PREPARED_ENV = "DR_IMAGES_PREPARED"


class ImagePreparationError(RuntimeError):
    pass


def prepared() -> bool:
    return os.environ.get(PREPARED_ENV) == "1"


def broker_image_tag() -> str:
    from .cluster import BROKER_REPO
    return os.environ.get("RUNE_IMAGE_TAG") or _git_sha(BROKER_REPO)


def _prepare_broker() -> None:
    from .cluster import COMPOSE_FILE, SERVICES
    from .dockerutil import Compose
    tag = broker_image_tag()
    image = f"rune-broker-node:{tag}"
    if os.environ.get("DR_E2E_BUILD", "0") == "1":
        with timing.phase("image.broker", image):
            res = Compose(COMPOSE_FILE, "rune-dr-prebuild", image_tag=tag).build(SERVICES[0], timeout=3600)
        if not res.ok:
            raise ImagePreparationError(f"building {image} failed:\n{res.stdout[-3000:]}{res.stderr[-3000:]}")
        return
    # Without DR_E2E_BUILD, `compose up` would silently BUILD a missing image inside the first test (and its
    # timeout); refuse up front instead.
    if not run(["docker", "image", "inspect", image], timeout=30).ok:
        raise ImagePreparationError(
            f"broker image {image} does not exist: build it (DR_E2E_BUILD=1, or `RUNE_IMAGE_TAG={tag} docker compose "
            f"-f docker/docker-compose.yml build rune-1`) or point RUNE_IMAGE_TAG at a built tag")


def prepare_all() -> None:
    """Build/verify every image once.  Raises ImagePreparationError naming the image."""
    from . import cluster as cluster_mod
    from . import driver, faults
    with timing.phase("image.prepare_all"):
        _prepare_broker()
        try:
            driver.ensure_dr_client_image()
        except Exception as e:  # noqa: BLE001 - re-raised with the image named
            raise ImagePreparationError(f"dr client image {driver.DR_IMAGE}: {e}") from e
        try:
            cluster_mod.ensure_ql_image()
        except Exception as e:  # noqa: BLE001
            raise ImagePreparationError(f"rune CLI image: {e}") from e
        try:
            faults.ensure_netem_image()
        except Exception as e:  # noqa: BLE001
            raise ImagePreparationError(f"netem image {faults.NETEM_IMAGE}: {e}") from e
        if not run(["docker", "image", "inspect", "alpine:latest"], timeout=30).ok:
            res = run(["docker", "pull", "alpine:latest"], timeout=600)
            if not res.ok:
                raise ImagePreparationError(f"docker pull alpine:latest failed:\n{res.stderr}")
    os.environ[PREPARED_ENV] = "1"
