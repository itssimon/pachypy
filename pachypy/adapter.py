import os
import time
import json
from typing import List, Dict, Generator, Union, Optional, TypeVar, Any, cast

import grpc
import pandas as pd
from python_pachyderm.client.pfs.pfs_pb2 import (
    Repo, Commit, Branch, File,
    ListRepoRequest, ListCommitRequest, ListBranchRequest, GlobFileRequest, GetFileRequest,
    CreateRepoRequest, DeleteRepoRequest,
    CreateBranchRequest, DeleteBranchRequest,
    StartCommitRequest, FinishCommitRequest, DeleteCommitRequest, FlushCommitRequest,
    PutFileRequest, DeleteFileRequest,
    RESERVED as FILETYPE_RESERVED, FILE as FILETYPE_FILE, DIR as FILETYPE_DIR,
    NONE as DELIMITER_NONE, JSON as DELIMITER_JSON, LINE as DELIMITER_LINE, SQL as DELIMITER_SQL, CSV as DELIMITER_CSV
)
from python_pachyderm.client.pfs.pfs_pb2_grpc import APIStub as PfsAPIStub
from python_pachyderm.client.pps.pps_pb2 import (
    Pipeline, Job, Input, Transform,
    ListPipelineRequest, ListJobRequest, ListDatumRequest, GetLogsRequest, DeleteJobRequest,
    CreatePipelineRequest, DeletePipelineRequest, StartPipelineRequest, StopPipelineRequest, InspectPipelineRequest,
    FAILED as DATUM_FAILED, SUCCESS as DATUM_SUCCESS, SKIPPED as DATUM_SKIPPED, STARTING as DATUM_STARTING,
    JOB_STARTING, JOB_RUNNING, JOB_FAILURE, JOB_SUCCESS, JOB_KILLED,
    PIPELINE_STARTING, PIPELINE_RUNNING, PIPELINE_RESTARTING, PIPELINE_FAILURE, PIPELINE_PAUSED, PIPELINE_STANDBY,
)
from python_pachyderm.client.pps.pps_pb2_grpc import APIStub as PpsAPIStub
from python_pachyderm.client.version.versionpb.version_pb2 import Version
from python_pachyderm.client.version.versionpb.version_pb2_grpc import APIStub as VersionAPIStub


T = TypeVar('T')


class PachydermError(Exception):

    def __init__(self, message: str, code=None):
        super().__init__(message)
        try:
            self.status_code = code.value[0]
            self.status = code.value[1]
        except (AttributeError, KeyError):
            self.status_code = None
            self.status = None


def retry(f: T) -> T:
    def retry_wrapper(self, *args, **kwargs):
        adapter = self.adapter if hasattr(self, 'adapter') else self
        try:
            return f(self, *args, **kwargs)
        except grpc._channel._Rendezvous as e:
            if e.code().value[1] == 'unavailable' and adapter._retries < adapter.max_retries:
                if adapter.check_connectivity():
                    adapter._retries += 1
                    return retry_wrapper(self, *args, **kwargs)
            raise PachydermError(e.details(), e.code())
        else:
            adapter._retries = 0
            adapter._connectable = True
    return cast(T, retry_wrapper)


class PachydermAdapter:

    """Adapter class handling communication with Pachyderm.

    It is effectively a wrapper around the python_pachyderm package.
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
        if host is not None and ':' in host:
            host_split = host.split(':')
            host = host_split[0]
            port = int(host_split[1])

        self.host = host or 'localhost'
        self.port = port or 30650
        self.channel = grpc.insecure_channel(f'{self.host}:{self.port}')
        self.pfs_stub = PfsAPIStub(self.channel)
        self.pps_stub = PpsAPIStub(self.channel)
        self.version_stub = VersionAPIStub(self.channel)

        self._retries = 0
        self._max_retries = 1
        self._connectable: Optional[bool] = None

    def check_connectivity(self, timeout: float = 10.0) -> bool:
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
        connectivity = self.channel._channel.check_connectivity_state(True)
        while connectivity < 2:
            if time.time() > timeout:
                connectivity = 5
                break
            time.sleep(0.001)
            connectivity = self.channel._channel.check_connectivity_state(False)
        self._connectable = connectivity == 2
        return self._connectable

    @retry
    def list_repos(self) -> pd.DataFrame:
        res = []
        for repo in self.pfs_stub.ListRepo(ListRepoRequest()).repo_info:
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
        repos = self.pfs_stub.ListRepo(ListRepoRequest()).repo_info
        return [r.repo.name for r in repos]

    @retry
    def list_branch_heads(self, repo: str) -> Dict[str, str]:
        branches = self.pfs_stub.ListBranch(ListBranchRequest(repo=Repo(name=repo))).branch_info
        return {b.branch.name: b.head.id for b in branches}

    @retry
    def list_commits(self, repo: str, n: int = 20) -> pd.DataFrame:
        i = 1
        res = []
        commit_branches = _invert_dict(self.list_branch_heads(repo))
        stream = self.pfs_stub.ListCommitStream(ListCommitRequest(repo=Repo(name=repo)))
        for commit in stream:
            res.append({
                'repo': commit.commit.repo.name,
                'commit': commit.commit.id,
                'parent_commit': commit.parent_commit.id,
                'branches': commit_branches.get(commit.commit.id, []),
                'size_bytes': commit.size_bytes,
                'started': _to_timestamp(commit.started.seconds, commit.started.nanos),
                'finished': _to_timestamp(commit.finished.seconds, commit.finished.nanos),
            })
            i += 1
            if n is not None and i > n:
                stream.cancel()
                break
        return pd.DataFrame(res, columns=['repo', 'commit', 'branches', 'size_bytes', 'started', 'finished', 'parent_commit']) \
            .astype({'size_bytes': 'int', 'started': 'datetime64[ns]', 'finished': 'datetime64[ns]'})

    @retry
    def list_files(self, repo: str, branch: Optional[str] = 'master', commit: Optional[str] = None, glob: str = '**') -> pd.DataFrame:
        if branch is None and commit is None:
            raise ValueError('branch and commit cannot both be None')
        file_type_mapping = {
            FILETYPE_RESERVED: 'reserved',
            FILETYPE_FILE: 'file',
            FILETYPE_DIR: 'dir',
        }
        res = []
        branch_heads = self.list_branch_heads(repo)
        commit_branches = _invert_dict(branch_heads)
        if commit is None and branch is not None:
            commit = branch_heads.get(branch)
        if commit is not None:
            commit = Commit(repo=Repo(name=repo), id=commit)
            for file in self.pfs_stub.GlobFileStream(GlobFileRequest(commit=commit, pattern=glob)):
                res.append({
                    'repo': file.file.commit.repo.name,
                    'commit': file.file.commit.id,
                    'branches': commit_branches.get(file.file.commit.id, []),
                    'path': file.file.path,
                    'type': file_type_mapping.get(file.file_type, 'unknown'),
                    'size_bytes': file.size_bytes,
                    'committed': _to_timestamp(file.committed.seconds, file.committed.nanos),
                })
        return pd.DataFrame(res, columns=['repo', 'commit', 'branches', 'path', 'type', 'size_bytes', 'committed']) \
            .astype({'size_bytes': 'int', 'committed': 'datetime64[ns]'})

    @retry
    def list_pipelines(self) -> pd.DataFrame:
        state_mapping = {
            PIPELINE_STARTING: 'starting',
            PIPELINE_RUNNING: 'running',
            PIPELINE_RESTARTING: 'restarting',
            PIPELINE_FAILURE: 'failure',
            PIPELINE_PAUSED: 'paused',
            PIPELINE_STANDBY: 'standby',
        }

        def input_string(i: Input) -> str:
            if i.cross:
                return '(' + ' ⨯ '.join([input_string(j) for j in i.cross]) + ')'
            elif i.union:
                return '(' + ' ∪ '.join([input_string(j) for j in i.union]) + ')'
            elif i.atom.name:
                name = i.atom.name + ('/' + i.atom.branch if i.atom.branch != 'master' else '')
                return str(name + ':' + i.atom.glob)
            elif i.pfs.name:
                name = i.pfs.name + ('/' + i.pfs.branch if i.pfs.branch != 'master' else '')
                return str(name + ':' + i.pfs.glob)
            elif i.cron.name:
                return str(i.cron.name)
            elif i.git.name:
                return str(i.git.name + ('/' + i.git.branch if i.git.branch != 'master' else ''))
            else:
                return '?'

        def input_repos(i: Input) -> Generator[str, None, None]:
            cross_or_union = i.cross or i.union
            if cross_or_union:
                for j in cross_or_union:
                    yield from input_repos(j)
            elif i.atom.repo:
                yield i.atom.repo
            elif i.pfs.repo:
                yield i.pfs.repo

        res = []
        pipelines = self.pps_stub.ListPipeline(ListPipelineRequest()).pipeline_info
        for pipeline in pipelines:
            res.append({
                'pipeline': pipeline.pipeline.name,
                'image': pipeline.transform.image,
                'cron_spec': ', '.join([cron['spec'] for cron in _pipeline_input_cron_specs(pipeline.input)]),
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
        pipelines = self.pps_stub.ListPipeline(ListPipelineRequest()).pipeline_info
        return [p.pipeline.name for p in pipelines]

    @retry
    def list_jobs(self, pipeline: Optional[str] = None, n: int = 20) -> pd.DataFrame:
        state_mapping = {
            JOB_STARTING: 'starting',
            JOB_RUNNING: 'running',
            JOB_FAILURE: 'failure',
            JOB_SUCCESS: 'success',
            JOB_KILLED: 'killed',
        }
        i = 1
        res = []
        jobs_stream = self.pps_stub.ListJobStream(ListJobRequest(pipeline=Pipeline(name=pipeline)))
        for job in jobs_stream:
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
                jobs_stream.cancel()
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
        state_mapping = {
            DATUM_FAILED: 'failed',
            DATUM_SUCCESS: 'success',
            DATUM_SKIPPED: 'skipped',
            DATUM_STARTING: 'starting',
        }
        file_type_mapping = {
            FILETYPE_RESERVED: 'reserved',
            FILETYPE_FILE: 'file',
            FILETYPE_DIR: 'dir',
        }
        res = []
        datums_stream = self.pps_stub.ListDatumStream(ListDatumRequest(job=Job(id=job)))
        for datum in datums_stream:
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
        return pd.DataFrame(res, columns=['job', 'datum', 'state', 'repo', 'path', 'type', 'size_bytes', 'commit', 'committed']) \
            .astype({'size_bytes': 'int', 'committed': 'datetime64[ns]'})

    @retry
    def get_logs(self, pipeline: Optional[str] = None, job: Optional[str] = None, master: bool = False) -> pd.DataFrame:
        pipeline = Pipeline(name=pipeline) if pipeline else None
        job = Job(id=job) if job else None
        if pipeline is None and job is None:
            raise ValueError('One of `pipeline` or `job` must be specified')
        res = []
        logs = self.pps_stub.GetLogs(GetLogsRequest(pipeline=pipeline, job=job, data_filters=tuple(), master=master))
        for msg in logs:
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
    def create_pipeline(self, pipeline_spec: dict) -> None:
        fields = {f.name for f in CreatePipelineRequest.DESCRIPTOR.fields}
        transform_fields = {f.name for f in Transform.DESCRIPTOR.fields}
        pipeline_spec = {k: v for k, v in pipeline_spec.items() if k in fields}
        pipeline_spec['transform'] = {k: v for k, v in pipeline_spec['transform'].items() if k in transform_fields}
        self.pps_stub.CreatePipeline(CreatePipelineRequest(**pipeline_spec))

    def update_pipeline(self, pipeline_spec: dict, reprocess: bool = False) -> None:
        pipeline_spec['update'] = True
        pipeline_spec['reprocess'] = reprocess
        self.create_pipeline(pipeline_spec)

    @retry
    def delete_pipeline(self, pipeline: str) -> None:
        self.pps_stub.DeletePipeline(DeletePipelineRequest(pipeline=Pipeline(name=pipeline)))

    @retry
    def start_pipeline(self, pipeline: str) -> None:
        self.pps_stub.StartPipeline(StartPipelineRequest(pipeline=Pipeline(name=pipeline)))

    @retry
    def stop_pipeline(self, pipeline: str) -> None:
        self.pps_stub.StopPipeline(StopPipelineRequest(pipeline=Pipeline(name=pipeline)))

    @retry
    def get_pipeline_cron_specs(self, pipeline: str) -> List[Dict[str, Any]]:
        res = self.pps_stub.InspectPipeline(InspectPipelineRequest(pipeline=Pipeline(name=pipeline)))
        return list(_pipeline_input_cron_specs(res.input))

    @retry
    def create_repo(self, repo: str, description: Optional[str] = None) -> None:
        self.pfs_stub.CreateRepo(CreateRepoRequest(repo=Repo(name=repo), description=description))

    @retry
    def delete_repo(self, repo: str, force: bool = False) -> None:
        self.pfs_stub.DeleteRepo(DeleteRepoRequest(repo=Repo(name=repo), force=force))

    @retry
    def delete_commit(self, repo: str, commit: str) -> None:
        self.pfs_stub.DeleteCommit(DeleteCommitRequest(commit=Commit(repo=Repo(name=repo), id=commit)))

    @retry
    def create_branch(self, repo: str, commit: str, branch: str) -> None:
        repo = Repo(name=repo)
        commit = Commit(repo=repo, id=commit)
        branch = Branch(repo=repo, name=branch)
        self.pfs_stub.CreateBranch(CreateBranchRequest(head=commit, branch=branch))

    @retry
    def delete_branch(self, repo: str, branch: str) -> None:
        self.pfs_stub.DeleteBranch(DeleteBranchRequest(branch=Branch(repo=Repo(name=repo), name=branch)))

    @retry
    def delete_job(self, job: str) -> None:
        self.pps_stub.DeleteJob(DeleteJobRequest(job=Job(id=job)))

    @retry
    def get_file(self, repo: str, path: str, branch: Optional[str] = 'master', commit: Optional[str] = None) -> Generator[bytes, None, None]:
        if commit is None and branch is not None:
            commit = self.list_branch_heads(repo).get(branch)
        file = File(commit=Commit(repo=Repo(name=repo), id=commit), path=path)
        response = self.pfs_stub.GetFile(GetFileRequest(file=file))
        for content in response:
            yield content.value

    @retry
    def get_version(self) -> str:
        version = self.version_stub.GetVersion(Version())
        return f'{version.major}.{version.minor}.{version.micro}'


class PachydermCommitAdapter:

    def __init__(self, adapter: PachydermAdapter, repo: str, branch: Optional[str] = 'master', parent_commit: Optional[str] = None, flush: bool = False):
        self.adapter = adapter
        self.repo = repo
        self.branch = branch
        self.parent_commit = parent_commit
        self.flush_ = flush
        self._commit = None
        self._finished = False
        self._buffer_size = 3 * 1024 * 1024  # 3 MB

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.delete()
        else:
            self.finish()
            if self.flush_:
                self.flush()

    @property
    def commit(self) -> Optional[str]:
        """Commit ID."""
        if self._commit is not None:
            return str(self._commit.id)
        else:
            return None

    @property
    def finished(self) -> bool:
        """Whether the commit is finished."""
        return self._finished

    @retry
    def start(self):
        """Start the commit."""
        self._raise_if_finished()
        parent_commit = Commit(repo=Repo(name=self.repo), id=self.parent_commit)
        self._commit = self.adapter.pfs_stub.StartCommit(StartCommitRequest(parent=parent_commit, branch=self.branch))

    @retry
    def finish(self):
        """Finish the commit."""
        self._raise_if_finished()
        self.adapter.pfs_stub.FinishCommit(FinishCommitRequest(commit=self._commit))
        self._finished = True

    @retry
    def flush(self, to_repos=None):
        """Blocks until all jobs triggered by this commit have finished.

        Args:
            to_repos: If specified, only the commits up to and including those repos
                will be considered, otherwise all repos are considered.
        """
        if to_repos:
            to_repos = [to_repos] if isinstance(to_repos, str) else to_repos
            to_repos = [Repo(name=repo) for repo in to_repos]
        res = self.adapter.pfs_stub.FlushCommit(FlushCommitRequest(commits=[self._commit], to_repos=to_repos or []))
        for _ in res:
            if res.done():
                return
            time.sleep(0.25)

    @retry
    def delete(self):
        """Delete the commit."""
        self.adapter.pfs_stub.DeleteCommit(DeleteCommitRequest(commit=self._commit))
        self._finished = True

    @retry
    def put_file_bytes(self, value: Union[str, bytes], path: str, encoding: str = 'utf-8',
                       delimiter: Optional[str] = None, target_file_datums: int = 0, target_file_bytes: int = 0) -> None:
        """Uploads a string or bytes `value` to a file in the given `path` in PFS.

        Args:
            value: The value to upload. If a string is given, it will be encoded using `encoding` (default UTF-8).
            path: PFS path to upload file to. Needs to be the full PFS path including filename.
            delimiter: Causes data to be broken up into separate files with `path` as a prefix.
                Possible values are 'none' (default), 'json', 'line', 'sql' and 'csv'.
            target_file_datum: Specifies the target number of datums in each written file.
                It may be lower if data does not split evenly, but will never be higher, unless the value is 0 (default).
            target_file_bytes: Specifies the target number of bytes in each written file.
                Files may have more or fewer bytes than the target.
        """
        self._raise_if_finished()
        if isinstance(value, str):
            value = value.encode(encoding)
        delimiter = {
            None: DELIMITER_NONE,
            'none': DELIMITER_NONE,
            'json': DELIMITER_JSON,
            'line': DELIMITER_LINE,
            'sql': DELIMITER_SQL,
            'csv': DELIMITER_CSV
        }[delimiter.lower() if delimiter is not None else None]
        file = File(commit=self._commit, path=path)

        def _blocks(v):
            for i in range(0, len(v), self._buffer_size):
                yield PutFileRequest(file=file, value=v[i:i + self._buffer_size], delimiter=delimiter,
                                     target_file_datums=target_file_datums, target_file_bytes=target_file_bytes)

        self.adapter.pfs_stub.PutFile(_blocks(value))

    @retry
    def put_file_url(self, url: str, path: str, recursive: bool = False) -> None:
        """Uploads a file using the content found at a URL.

        The URL is sent to the server which performs the request.

        Args:
            url: The URL to download content from.
            path: PFS path to upload file to. Needs to be the full PFS path including filename.
            recursive: Allow recursive scraping of some URL types, e.g. on s3:// URLs.
        """
        self._raise_if_finished()
        file = File(commit=self._commit, path=path)
        self.adapter.pfs_stub.PutFile(iter([PutFileRequest(file=file, url=url, recursive=recursive)]))

    @retry
    def delete_file(self, path: str) -> None:
        """Deletes the file found in a given `path` in PFS, if it exists.

        Args:
            path: PFS path of file to delete.
        """
        self._raise_if_finished()
        self.adapter.pfs_stub.DeleteFile(DeleteFileRequest(file=File(commit=self._commit, path=path)))

    @retry
    def create_branch(self, branch: str) -> None:
        """Sets this commit as a branch.

        Args:
            branch: Name of the branch.
        """
        branch = Branch(repo=Repo(name=self.repo), name=branch)
        self.adapter.pfs_stub.CreateBranch(CreateBranchRequest(head=self._commit, branch=branch))

    @retry
    def _list_file_paths(self, glob: str) -> List[str]:
        return [str(f.file.path) for f in self.adapter.pfs_stub.GlobFileStream(GlobFileRequest(commit=self._commit, pattern=glob))]

    def _raise_if_finished(self):
        if self.finished:
            raise PachydermError(f'Commit {self.commit} is already finished')


def _pipeline_input_cron_specs(i: Input) -> Generator[Dict[str, Any], None, None]:
    if i.cron.spec != '':
        yield {
            'name': str(i.cron.name),
            'spec': str(i.cron.spec),
            'repo': str(i.cron.repo),
            'overwrite': bool(i.cron.overwrite),
        }
    cross_or_union = i.cross or i.union
    if cross_or_union:
        for j in cross_or_union:
            yield from _pipeline_input_cron_specs(j)


def _invert_dict(d: Dict[str, str]) -> Dict[str, List[str]]:
    inverted: Dict[str, List[str]] = {}
    for key, value in d.items():
        inverted.setdefault(value, []).append(key)
    return inverted


def _to_timestamp(seconds: int, nanos: int) -> Optional[pd.Timestamp]:
    if seconds > 0:
        return pd.Timestamp(float(f'{seconds}.{nanos}'), unit='s')
    else:
        return None


def _to_timedelta(seconds: int, nanos: int) -> pd.Timedelta:
    return pd.Timedelta(float(f'{seconds}.{nanos}'), unit='s')
