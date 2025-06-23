import threading
import pytest
import time

from requests import HTTPError

from fixtures.endpoint.http import EndpointHttpClient
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from google.protobuf.message import Message
from data import profile_pb2


def _start_profiling_cpu(client: EndpointHttpClient, event: threading.Event | None):
    """
    Start CPU profiling for the compute node.
    """
    log.info("Starting CPU profiling...")
    try:
        if event is not None:
            event.set()

        status, response = client.profile_cpu(100, 5, False)
        if status == 200:
            log.info("CPU profiling finished")
            profile = profile_pb2.Profile()
            Message.ParseFromString(profile, response)
            return profile
        elif status == 204:
            log.error("CPU profiling was stopped")
            raise HTTPError(f"Failed to finish CPU profiling: was stopped.")
        elif status == 208:
            log.error("CPU profiling is already in progress, nothing to do")
            raise HTTPError(f"Failed to finish CPU profiling: profiling is already in progress.")
    except Exception as e:
        log.error(f"Error starting CPU profiling: {e}")
        raise

def _stop_profiling_cpu(client: EndpointHttpClient, event: threading.Event | None):
    """
    Stop CPU profiling for the compute node.
    """
    log.info("Manually stopping CPU profiling...")
    try:
        if event is not None:
            event.set()

        status, response = client.profile_cpu(100, 5, True)
        if status == 200:
            log.info("CPU profiling stopped successfully")
        elif status == 412:
            log.info("CPU profiling is not running, nothing to do")
        else:
            log.error(f"Failed to stop CPU profiling: {status}: {response}")
            raise HTTPError(f"Failed to stop CPU profiling: {status}: {response}")
    except Exception as e:
        log.error(f"Error stopping CPU profiling: {e}")
        raise


def test_compute_profiling_cpu_with_timeout(neon_simple_env: NeonEnv):
    """
    Test that CPU profiling works correctly with timeout.
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    pg_conn = endpoint.connect()
    pg_cur = pg_conn.cursor()
    pg_cur.execute("create database profiling_test")
    http_client = endpoint.http_client()
    event = threading.Event()

    def _wait_and_assert_cpu_profiling():
        profile = _start_profiling_cpu(http_client, event)

        if profile is None:
            log.error("The received profiling data is malformed or empty.")
            return

        assert len(profile.sample) > 0, "No samples found in CPU profiling data"
        assert len(profile.mapping) > 0, "No mappings found in CPU profiling data"
        assert len(profile.location) > 0, "No locations found in CPU profiling data"
        assert len(profile.function) > 0, "No functions found in CPU profiling data"
        assert len(profile.string_table) > 0, "No string tables found in CPU profiling data"
        for string in [
            "PostgresMain",
            "ServerLoop",
            "BackgroundWorkerMain",
            "pq_recvbuf",
            "pq_getbyte",
        ]:
            assert string in profile.string_table, (
                f"Expected function '{string}' not found in string table"
            )

    thread = threading.Thread(target=_wait_and_assert_cpu_profiling)
    thread.start()

    def insert_rows():
        event.wait()  # Wait for profiling to be ready to start
        lfc_conn = endpoint.connect(dbname="profiling_test")
        lfc_cur = lfc_conn.cursor()
        n_records = 10000
        log.info(f"Inserting {n_records} rows")
        lfc_cur.execute(
            "create table t(pk integer primary key, payload text default repeat('?', 128))"
        )
        lfc_cur.execute(f"insert into t (pk) values (generate_series(1,{n_records}))")
        log.info(f"Inserted {n_records} rows")

    thread2 = threading.Thread(target=insert_rows)
    thread2.start()

    thread.join(timeout=60)
    thread2.join(timeout=60)

    endpoint.stop()
    endpoint.start()

def test_compute_profiling_cpu_start_and_stop(neon_simple_env: NeonEnv):
    """
    Test that CPU profiling can be started and stopped correctly.
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    http_client = endpoint.http_client()
    event = threading.Event()

    def _wait_and_assert_cpu_profiling():
        # Should raise as the profiling will be stopped.
        with pytest.raises(HTTPError) as _:
            _start_profiling_cpu(http_client, event)

    thread = threading.Thread(target=_wait_and_assert_cpu_profiling)
    thread.start()

    event.wait()  # Wait for profiling to be ready to start
    time.sleep(1)  # Give some time for the profiling to start
    _stop_profiling_cpu(http_client, None)

    thread.join(timeout=60)

    endpoint.stop()
    endpoint.start()


def test_compute_profiling_cpu_conflict(neon_simple_env: NeonEnv):
    pass

def test_compute_profiling_cpu_stop_twice(neon_simple_env: NeonEnv):
    pass
