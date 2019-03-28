import os
import time
import json
from datetime import datetime
from typing import Optional, List, Callable, Generator

import pandas as pd
from grpc._channel import _Rendezvous
from python_pachyderm import PpsClient, PfsClient
from python_pachyderm.client.pps.pps_pb2 import (
    Pipeline, Job,
    ListJobRequest, ListDatumRequest,
    CreatePipelineRequest, DeletePipelineRequest, StartPipelineRequest, StopPipelineRequest
)
from python_pachyderm.client.pfs.pfs_pb2 import (
    Repo, Commit,
    GlobFileRequest, ListCommitRequest
)
from python_pachyderm.pps_client import (
    FAILED, SUCCESS, SKIPPED, STARTING,
    JOB_STARTING, JOB_RUNNING, JOB_FAILURE, JOB_SUCCESS, JOB_KILLED,
    PIPELINE_STARTING, PIPELINE_RUNNING, PIPELINE_RESTARTING, PIPELINE_FAILURE, PIPELINE_PAUSED, PIPELINE_STANDBY
)
from python_pachyderm.pfs_client import (
    RESERVED, FILE, DIR
)


class PachydermException(Exception):

    def __init__(self, details: str, code):
        super().__init__(details)
        self.status_code = code.value[0]
        self.status = code.value[1]


def retry(f: Callable):
    def retry_wrapper(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except _Rendezvous as e:
            if e.code().value[1] == 'unavailable' and self._retries < self.max_retries:
                if self.check_connectivity():
                    self._retries += 1
                    return retry_wrapper(self, *args, **kwargs)
            raise PachydermException(e.details(), e.code())
        else:
            self._retries = 0
    return retry_wrapper


class PachydermAdapter:

    """Client adapter class handling communication with Pachyderm.

    It is effectively a wrapper around the python_pachyderm package.
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
            host_split = host.split(':')
            host = host_split[0]
            port = int(host_split[1])

        kwargs = {}
        if host is not None:
            kwargs['host'] = host
        if port is not None:
            kwargs['port'] = port
        self.pps_client = PpsClient(**kwargs)
        self.pfs_client = PfsClient(**kwargs)
        self.max_retries = 1
        self._retries = 0

    @property
    def host(self) -> str:
        return self.pps_client.channel._channel.target().decode().split(':')[0]

    @property
    def port(self) -> int:
        return int(self.pps_client.channel._channel.target().decode().split(':')[1])

    def check_connectivity(self, timeout: int = 10) -> bool:
        """Checks the connectivity to pachd. Tries to connect if not currently connected.

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
        return connectivity == 2

    @retry
    def list_repos(self) -> pd.DataFrame:
        """Returns list of repositories."""
        res = []
        for repo in self.pfs_client.list_repo():
            res.append({
                'repo': repo.repo.name,
                'size_bytes': repo.size_bytes,
                'branches': [b.name for b in repo.branches],
                'created': _to_timestamp(repo.created.seconds, repo.created.nanos),
            })
        return pd.DataFrame(res, columns=['repo', 'size_bytes', 'branches', 'created']) \
            .astype({'size_bytes': 'int', 'created': 'datetime64[ns]'})

    @retry
    def list_repo_names(self) -> List[str]:
        return [r.repo.name for r in self.pfs_client.list_repo()]

    @retry
    def list_commits(self, repo: str, n: int = 20) -> pd.DataFrame:
        """Returns list of commits.

        Args:
            repo: Name of repo to list commits for.
        """
        i = 1
        res = []
        for commit in self.pfs_client.stub.ListCommitStream(ListCommitRequest(repo=Repo(name=repo))):
            res.append({
                'commit': commit.commit.id,
                'parent_commit': commit.parent_commit.id,
                'repo': commit.commit.repo.name,
                'size_bytes': commit.size_bytes,
                'started': _to_timestamp(commit.started.seconds, commit.started.nanos),
                'finished': _to_timestamp(commit.finished.seconds, commit.finished.nanos),
            })
            i += 1
            if n is not None and i > n:
                break
        return pd.DataFrame(res, columns=['repo', 'commit', 'size_bytes', 'started', 'finished', 'parent_commit']) \
            .astype({'size_bytes': 'int', 'started': 'datetime64[ns]', 'finished': 'datetime64[ns]'})

    @retry
    def get_last_commit(self, repo: str) -> Optional[str]:
        """Returns the ID of the last commit to a repo.

        Args:
            repo: Repo to get commit ID for.
        """
        try:
            commit = next(self.pfs_client.stub.ListCommitStream(ListCommitRequest(repo=Repo(name=repo))))
            return commit.commit.id
        except StopIteration:
            return None

    @retry
    def list_files(self, repo: str, commit: str = None, pattern: str = '**') -> pd.DataFrame:
        """Returns list of files.

        Args:
            repo: Name of repo to list files from.
            commit: Commit ID to list files from.
            pattern: Glob pattern to filter files returned.
        """
        file_type_mapping = {
            RESERVED: 'reserved',
            FILE: 'file',
            DIR: 'dir',
        }
        res = []
        if commit is None:
            commit = self.get_last_commit(repo)
        if commit is not None:
            commit = Commit(repo=Repo(name=repo), id=commit)
            for file in self.pfs_client.stub.GlobFileStream(GlobFileRequest(commit=commit, pattern=pattern)):
                res.append({
                    'repo': file.file.commit.repo.name,
                    'commit': file.file.commit.id,
                    'path': file.file.path,
                    'type': file_type_mapping.get(file.file_type, 'unknown'),
                    'size_bytes': file.size_bytes,
                    'committed': _to_timestamp(file.committed.seconds, file.committed.nanos),
                })
        return pd.DataFrame(res, columns=['repo', 'commit', 'path', 'type', 'size_bytes', 'committed']) \
            .astype({'size_bytes': 'int', 'committed': 'datetime64[ns]'})

    @retry
    def list_pipelines(self) -> pd.DataFrame:
        """Returns list of pipelines."""
        state_mapping = {
            PIPELINE_STARTING: 'starting',
            PIPELINE_RUNNING: 'running',
            PIPELINE_RESTARTING: 'restarting',
            PIPELINE_FAILURE: 'failure',
            PIPELINE_PAUSED: 'paused',
            PIPELINE_STANDBY: 'standby',
        }

        def cron_spec(i) -> str:
            if i.cron.spec != '':
                return i.cron.spec
            cross_or_union = i.cross or i.union
            if cross_or_union:
                for j in cross_or_union:
                    spec = cron_spec(j)
                    if spec:
                        return spec
            return ''

        def input_string(i) -> str:
            if i.cross:
                return '(' + ' ⨯ '.join([input_string(j) for j in i.cross]) + ')'
            elif i.union:
                return '(' + ' ∪ '.join([input_string(j) for j in i.union]) + ')'
            elif i.atom.name:
                name = i.atom.name + ('/' + i.atom.branch if i.atom.branch != 'master' else '')
                return name + ':' + i.atom.glob
            elif i.pfs.name:
                name = i.pfs.name + ('/' + i.pfs.branch if i.pfs.branch != 'master' else '')
                return name + ':' + i.pfs.glob
            elif i.cron.name:
                return i.cron.name
            elif i.git.name:
                return i.git.name + ('/' + i.git.branch if i.git.branch != 'master' else '')
            else:
                return '?'

        def input_repos(i) -> Generator[str, None, None]:
            cross_or_union = i.cross or i.union
            if cross_or_union:
                for j in cross_or_union:
                    yield from input_repos(j)
            elif i.atom.repo:
                yield i.atom.repo
            elif i.pfs.repo:
                yield i.pfs.repo

        res = []
        for pipeline in self.pps_client.list_pipeline().pipeline_info:
            res.append({
                'pipeline': pipeline.pipeline.name,
                'image': pipeline.transform.image,
                'cron_spec': cron_spec(pipeline.input),
                'input': input_string(pipeline.input),
                'input_repos': list(input_repos(pipeline.input)),
                'output_branch': pipeline.output_branch,
                'parallelism_constant': pipeline.parallelism_spec.constant,
                'parallelism_coefficient': pipeline.parallelism_spec.coefficient,
                'datum_tries': pipeline.datum_tries,
                'max_queue_size': pipeline.max_queue_size,
                'jobs_running': pipeline.job_counts[JOB_RUNNING],
                'jobs_success': pipeline.job_counts[JOB_SUCCESS],
                'jobs_failure': pipeline.job_counts[JOB_FAILURE],
                'created': _to_timestamp(pipeline.created_at.seconds, pipeline.created_at.nanos),
                'state': state_mapping.get(pipeline.state, 'unknown'),
            })
        return pd.DataFrame(res, columns=[
            'pipeline', 'state', 'image', 'cron_spec', 'input', 'input_repos', 'output_branch',
            'parallelism_constant', 'parallelism_coefficient', 'datum_tries', 'max_queue_size',
            'jobs_running', 'jobs_success', 'jobs_failure', 'created',
        ]).astype({
            'parallelism_constant': 'int',
            'parallelism_coefficient': 'float',
            'datum_tries': 'int',
            'jobs_running': 'int',
            'jobs_success': 'int',
            'jobs_failure': 'int',
            'created': 'datetime64[ns]',
        })

    @retry
    def list_pipeline_names(self) -> List[str]:
        return [p.pipeline.name for p in self.pps_client.list_pipeline().pipeline_info]

    @retry
    def list_jobs(self, pipeline: Optional[str] = None, n: int = 20) -> pd.DataFrame:
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
                'started': _to_timestamp(job.started.seconds, job.started.nanos),
                'finished': _to_timestamp(job.finished.seconds, job.finished.nanos),
                'restart': job.restart,
                'data_processed': job.data_processed,
                'data_skipped': job.data_skipped,
                'data_total': job.data_total,
                'download_time': _to_timedelta(job.stats.download_time.seconds, job.stats.download_time.nanos),
                'process_time': _to_timedelta(job.stats.process_time.seconds, job.stats.process_time.nanos),
                'upload_time': _to_timedelta(job.stats.upload_time.seconds, job.stats.upload_time.nanos),
                'download_bytes': job.stats.download_bytes,
                'upload_bytes': job.stats.upload_bytes,
                'output_commit': job.output_commit.id,
            })
            i += 1
            if n is not None and i > n:
                break
        return pd.DataFrame(res, columns=[
            'job', 'pipeline', 'state', 'started', 'finished', 'restart',
            'data_processed', 'data_skipped', 'data_total',
            'download_time', 'process_time', 'upload_time',
            'download_bytes', 'upload_bytes', 'output_commit'
        ]).astype({
            'started': 'datetime64[ns]',
            'finished': 'datetime64[ns]',
            'restart': 'int',
            'data_processed': 'int',
            'data_skipped': 'int',
            'data_total': 'int',
            'download_time': 'timedelta64[ns]',
            'process_time': 'timedelta64[ns]',
            'upload_time': 'timedelta64[ns]',
            'download_bytes': 'float',
            'upload_bytes': 'float',
        })

    @retry
    def list_datums(self, job: str) -> pd.DataFrame:
        """Returns a list of datums and files for a given job.

        Args:
            job: Job ID to list datums for.
        """
        state_mapping = {
            FAILED: 'failed',
            SUCCESS: 'success',
            SKIPPED: 'skipped',
            STARTING: 'starting',
        }
        file_type_mapping = {
            RESERVED: 'reserved',
            FILE: 'file',
            DIR: 'dir',
        }
        res = []
        for datum in self.pps_client.stub.ListDatumStream(ListDatumRequest(job=Job(id=job))):
            for data in datum.datum_info.data:
                res.append({
                    'job': datum.datum_info.datum.job.id,
                    'datum': datum.datum_info.datum.id,
                    'state': state_mapping.get(datum.datum_info.state, 'unknown'),
                    'repo': data.file.commit.repo.name,
                    'commit': data.file.commit.id,
                    'path': data.file.path,
                    'type': file_type_mapping.get(data.file_type, 'unknown'),
                    'size_bytes': data.size_bytes,
                    'committed': _to_timestamp(data.committed.seconds, data.committed.nanos),
                })
        return pd.DataFrame(res, columns=['job', 'datum', 'state', 'repo', 'commit', 'path', 'type', 'size_bytes', 'committed']) \
            .astype({'size_bytes': 'int', 'committed': 'datetime64[ns]'})

    @retry
    def get_logs(self, pipeline: Optional[str] = None, job: Optional[str] = None, master: bool = False) -> pd.DataFrame:
        """Returns log entries.

        Args:
            pipeline: Name of pipeline to filter logs by.
            job: ID of job to filter logs by. (optional)
            master: Whether to return logs from the Pachyderm master process.
        """
        res = []
        for msg in self.pps_client.get_logs(pipeline_name=pipeline, job_id=job, master=master):
            message = msg.message.strip()
            if message:
                res.append({
                    'pipeline': msg.pipeline_name,
                    'job': msg.job_id,
                    'ts': _to_timestamp(msg.ts.seconds, msg.ts.nanos),
                    'message': message,
                    'worker': msg.worker_id,
                    'datum': msg.datum_id,
                    'user': msg.user,
                })
        return pd.DataFrame(res, columns=[
            'pipeline', 'job', 'ts', 'message',
            'worker', 'datum', 'user'
        ]).astype({
            'ts': 'datetime64[ns]',
            'user': 'bool',
        })

    @retry
    def create_pipeline(self, pipeline_specs: dict) -> None:
        """Create pipeline with given specs.

        Args:
            pipeline_specs: Pipeline specs.
        """
        self.pps_client.stub.CreatePipeline(CreatePipelineRequest(**pipeline_specs))

    @retry
    def update_pipeline(self, pipeline_specs: dict, reprocess: bool = False) -> None:
        """Update existing pipeline with given specs.

        Args:
            pipeline_specs: Pipeline specs.
            reprocess: Whether to reprocess datums with updated pipeline.
        """
        self.pps_client.stub.CreatePipeline(CreatePipelineRequest(update=True, reprocess=reprocess, **pipeline_specs))

    @retry
    def delete_pipeline(self, pipeline: str) -> None:
        """Delete pipeline.

        Args:
            pipeline: Name of pipeline to delete.
        """
        self.pps_client.stub.DeletePipeline(DeletePipelineRequest(pipeline=Pipeline(name=pipeline)))

    @retry
    def start_pipeline(self, pipeline: str) -> None:
        """Restart stopped pipeline.

        Args:
            pipeline: Name of pipeline to start.
        """
        self.pps_client.stub.StartPipeline(StartPipelineRequest(pipeline=Pipeline(name=pipeline)))

    @retry
    def stop_pipeline(self, pipeline: str) -> None:
        """Stop pipeline.

        Args:
            pipeline: Name of pipeline to stop.
        """
        self.pps_client.stub.StopPipeline(StopPipelineRequest(pipeline=Pipeline(name=pipeline)))

    @retry
    def create_repo(self, repo: str, description: Optional[str] = None) -> None:
        """Create new repository in pfs.

        Args:
            repo: Name of new repository.
            description: Repository description.
        """
        self.pfs_client.create_repo(repo, description=description)

    @retry
    def delete_repo(self, repo: str) -> None:
        """Delete repository.

        Args:
            repo: Name of repository to delete.
        """
        self.pfs_client.delete_repo(repo)

    @retry
    def commit_timestamp_file(self, repo: str, branch: str = 'master', overwrite: bool = True) -> None:
        """Commits a timestamp file to given repository to trigger a cron input.

        Args:
            repo: Name of repository
            branch: Name of branch. Defaults to 'master'.
            overwrite: Whether to overwrite an existing timestamp file or to write a new one (Pachyderm >=1.8.6)
        """
        if overwrite:
            timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            commit = self.pfs_client.start_commit(repo, branch=branch)
            self.pfs_client.delete_file(commit, 'time')
            self.pfs_client.put_file_bytes(commit, 'time', json.dumps(timestamp).encode('utf-8'))
            self.pfs_client.finish_commit(commit)
        else:
            raise NotImplementedError


def _to_timestamp(seconds: int, nanos: int) -> pd.Timestamp:
    return pd.Timestamp(float(f'{seconds}.{nanos}'), unit='s')


def _to_timedelta(seconds: int, nanos: int) -> pd.Timedelta:
    return pd.Timedelta(float(f'{seconds}.{nanos}'), unit='s')
