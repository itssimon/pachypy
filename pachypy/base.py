import os
import time
import json
from typing import Optional

import pandas as pd
from grpc._channel import _Rendezvous

from python_pachyderm import PpsClient, PfsClient
from python_pachyderm.client.pps.pps_pb2 import ListJobRequest, CreatePipelineRequest, DeletePipelineRequest, Pipeline
from python_pachyderm.pps_client import JOB_STARTING, JOB_RUNNING, JOB_FAILURE, JOB_SUCCESS, JOB_KILLED
from python_pachyderm.pps_client import PIPELINE_STARTING, PIPELINE_RUNNING, PIPELINE_RESTARTING, PIPELINE_FAILURE, PIPELINE_PAUSED, PIPELINE_STANDBY


class PachydermException(Exception):

    def __init__(self, details, code):
        super().__init__(details)
        self.status_code = code.value[0]
        self.status = code.value[1]


def retry(f):
    def retry_wrapper(self, *args):
        try:
            return f(self, *args)
        except _Rendezvous as e:
            print('Caught exception')
            if e.code().value[1] == 'unavailable' and self._retries < self.max_retries:
                if self.check_connectivity():
                    self._retries += 1
                    return retry_wrapper(self, *args)
            raise PachydermException(e.details(), e.code())
        else:
            self._retries = 0
    return retry_wrapper


class PachydermWrapper:

    """Wrapper around client objects of the python_pachyderm package for easier interaction.

    This is the basis for the PachydermClient class and is not intended to be used directly.

    Args:
        host: Hostname or IP address to reach pachd. Attempts to get this from PACHD_ADDRESS or ``~/.pachyderm/config.json`` if not set.
        port: Port on which pachd is listening (usually 30650).
    """

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None):
        if host is None:
            host = os.getenv('PACHD_ADDRESS')
        if host is None:
            try:
                with open(os.path.expanduser('~/.pachyderm/config.json'), 'r') as f:
                    config = json.load(f)
                    host = config['v1']['pachd_address']
            except (json.JSONDecodeError, KeyError):
                pass
        if host is not None and port is None and ':' in host:
            try:
                host_split = host.split(':')
                host = host_split[0]
                port = int(host_split[1])
            except ValueError:
                pass
        self.host = host
        self.port = port

        kwargs = {}
        if host is not None:
            kwargs['host'] = host
        if port is not None:
            kwargs['port'] = port
        self.pps_client = PpsClient(**kwargs)
        self.pfs_client = PfsClient(**kwargs)
        self.max_retries = 3
        self._retries = 0

    def check_connectivity(self, timeout: int = 10) -> bool:
        """Checks the connectivity to pachd.

        The gRPC channel connectivity knows 5 states:
        0 = idle, 1 = connecting, 2 = ready, 3 = transient failure, 4 = shutdown.

        Args:
            timeout: Timeout in seconds.

        Returns:
            True if the connectivity state is ready (2), False otherwise.
        """
        connectivity = 0
        timeout = time.time() + timeout
        connectivity = self.pfs_client.channel._channel.check_connectivity_state(True)
        while connectivity < 2:
            if time.time() > timeout:
                connectivity = 5
                break
            time.sleep(0.001)
            connectivity = self.pfs_client.channel._channel.check_connectivity_state(False)
        # 0 = idle, 1 = connecting, 2 = ready, 3 = transient failure, 4 = shutdown
        return connectivity == 2

    @retry
    def _list_repos(self) -> pd.DataFrame:
        """Returns list of repositories."""
        res = []
        for repo in self.pfs_client.list_repo():
            res.append({
                'repo': repo.repo.name,
                'size_bytes': repo.size_bytes,
                'branches': len(repo.branches),
                'created': repo.created.seconds,
            })
        return pd.DataFrame(res, columns=['repo', 'size_bytes', 'branches', 'created'])

    @retry
    def _list_pipelines(self) -> pd.DataFrame:
        """Returns list of pipelines."""
        state_mapping = {
            PIPELINE_STARTING: 'starting',
            PIPELINE_RUNNING: 'running',
            PIPELINE_RESTARTING: 'restarting',
            PIPELINE_FAILURE: 'failure',
            PIPELINE_PAUSED: 'paused',
            PIPELINE_STANDBY: 'standby',
        }

        def cron_spec(i):
            if i.cron.spec != '':
                return i.cron.spec
            cross_or_union = i.cross or i.union
            if cross_or_union:
                for j in cross_or_union:
                    spec = cron_spec(j)
                    if spec:
                        return spec
            return ''

        def format_input(i):
            if i.cross:
                return '(' + ' ⨯ '.join([format_input(j) for j in i.cross]) + ')'
            elif i.union:
                return '(' + ' ∪ '.join([format_input(j) for j in i.union]) + ')'
            elif i.atom.name:
                name = i.atom.name + ('/' + i.atom.branch if i.atom.branch != 'master' else '')
                return f'{name}:{i.atom.glob}'
            elif i.pfs.name:
                name = i.pfs.name + ('/' + i.pfs.branch if i.pfs.branch != 'master' else '')
                return f'{name}:{i.pfs.glob}'
            elif i.cron.name:
                return i.cron.name
            elif i.git.name:
                return i.git.name + ('/' + i.git.branch if i.git.branch != 'master' else '')
            else:
                return '?'

        def dependencies(i):
            cross_or_union = i.cross or i.union
            if cross_or_union:
                for j in cross_or_union:
                    yield from dependencies(j)
            elif i.atom.repo:
                yield i.atom.repo
            elif i.pfs.repo:
                yield i.pfs.repo

        i = 1
        res = []
        for pipeline in self.pps_client.list_pipeline().pipeline_info:
            res.append({
                'pipeline': pipeline.pipeline.name,
                'image': pipeline.transform.image,
                'cron_spec': cron_spec(pipeline.input),
                'input': format_input(pipeline.input),
                'input_repos': list(dependencies(pipeline.input)),
                'output_branch': pipeline.output_branch,
                'parallelism_constant': pipeline.parallelism_spec.constant,
                'parallelism_coefficient': pipeline.parallelism_spec.coefficient,
                'datum_tries': pipeline.datum_tries,
                'jobs_running': pipeline.job_counts[JOB_RUNNING],
                'jobs_success': pipeline.job_counts[JOB_SUCCESS],
                'jobs_failure': pipeline.job_counts[JOB_FAILURE],
                'created': float(f'{pipeline.created_at.seconds}.{pipeline.created_at.nanos}'),
                'state': state_mapping.get(pipeline.state, 'unknown'),
                'sort': i
            })
            i += 1
        return pd.DataFrame(res, columns=[
            'pipeline', 'image', 'cron_spec', 'input', 'input_repos', 'output_branch',
            'parallelism_constant', 'parallelism_coefficient', 'datum_tries',
            'jobs_running', 'jobs_success', 'jobs_failure',
            'created', 'state', 'sort'
        ])

    @retry
    def _list_jobs(self, pipeline: Optional[str] = None, n: int = 20) -> pd.DataFrame:
        """Returns list of last n jobs.

        Args:
            pipeline: Name of pipeline to return jobs for. Returns all jobs if not specified.
            n: Maximum number of jobs to return.
        """
        state_mapping = {
            JOB_STARTING: 'starting',
            JOB_RUNNING: 'running',
            JOB_FAILURE: 'failure',
            JOB_SUCCESS: 'success',
            JOB_KILLED: 'killed',
        }
        i = 1
        res = []
        for job in self.pps_client.stub.ListJobStream(ListJobRequest(pipeline=Pipeline(name=pipeline))):
            res.append({
                'job': job.job.id,
                'pipeline': job.pipeline.name,
                'state': state_mapping.get(job.state, 'unknown'),
                'started': float(f'{job.started.seconds}.{job.started.nanos}'),
                'finished': float(f'{job.finished.seconds}.{job.finished.nanos}'),
                'restart': job.restart,
                'data_processed': job.data_processed,
                'data_skipped': job.data_skipped,
                'data_total': job.data_total,
                'download_time': float(f'{job.stats.download_time.seconds}.{job.stats.download_time.nanos}'),
                'process_time': float(f'{job.stats.process_time.seconds}.{job.stats.process_time.nanos}'),
                'upload_time': float(f'{job.stats.upload_time.seconds}.{job.stats.upload_time.nanos}'),
                'download_bytes': job.stats.download_bytes,
                'upload_bytes': job.stats.upload_bytes
            })
            i += 1
            if n is not None and i > n:
                break
        return pd.DataFrame(res, columns=[
            'job', 'pipeline', 'state', 'started', 'finished', 'restart',
            'data_processed', 'data_skipped', 'data_total',
            'download_time', 'process_time', 'upload_time',
            'download_bytes', 'upload_bytes'
        ])

    @retry
    def _get_logs(self, pipeline: Optional[str] = None, job: Optional[str] = None, master: bool = False) -> pd.DataFrame:
        """Returns log entries.

        Args:
            pipeline: Name of pipeline to filter logs by.
            job: ID of job to filter logs by. (optional)
            master: Whether to return logs from the Pachyderm master process.
        """
        res = []
        for msg in self.pps_client.get_logs(pipeline_name=pipeline, job_id=job, master=master):
            res.append({
                'pipeline': msg.pipeline_name,
                'job': msg.job_id,
                'ts': float(f'{msg.ts.seconds}.{msg.ts.nanos}'),
                'message': msg.message,
                'worker': msg.worker_id,
                'datum': msg.datum_id,
                'user': msg.user,
            })
        return pd.DataFrame(res, columns=[
            'pipeline', 'job', 'ts', 'message',
            'worker', 'datum', 'user'
        ])

    @retry
    def _create_pipeline(self, pipeline_specs: dict) -> None:
        """Create pipeline with given specs.

        Args:
            pipeline_specs: Pipeline specs.
        """
        self.pps_client.stub.CreatePipeline(CreatePipelineRequest(**pipeline_specs))

    @retry
    def _update_pipeline(self, pipeline_specs: dict, reprocess: bool = False) -> None:
        """Update existing pipeline with given specs.

        Args:
            pipeline_specs: Pipeline specs.
            reprocess: Whether to reprocess datums with updated pipeline.
        """
        self.pps_client.stub.CreatePipeline(CreatePipelineRequest(update=True, reprocess=reprocess, **pipeline_specs))

    @retry
    def _delete_pipeline(self, pipeline: str) -> None:
        """Delete pipeline.

        Args:
            pipeline: Name of pipeline to delete.
        """
        self.pps_client.stub.DeletePipeline(DeletePipelineRequest(pipeline=Pipeline(name=pipeline)))
