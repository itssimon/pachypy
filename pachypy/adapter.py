import os
import time
import json
from typing import List, Dict, Generator, Union, Optional

import grpc
import pandas as pd
from python_pachyderm.client.pps.pps_pb2 import (
    Pipeline, Job,
    ListPipelineRequest, ListJobRequest, ListDatumRequest, GetLogsRequest,
    CreatePipelineRequest, DeletePipelineRequest, StartPipelineRequest, StopPipelineRequest,
    FAILED as DATUM_FAILED, SUCCESS as DATUM_SUCCESS, SKIPPED as DATUM_SKIPPED, STARTING as DATUM_STARTING,
    JOB_STARTING, JOB_RUNNING, JOB_FAILURE, JOB_SUCCESS, JOB_KILLED,
    PIPELINE_STARTING, PIPELINE_RUNNING, PIPELINE_RESTARTING, PIPELINE_FAILURE, PIPELINE_PAUSED, PIPELINE_STANDBY,
)
from python_pachyderm.client.pps.pps_pb2_grpc import APIStub as PpsAPIStub
from python_pachyderm.client.pfs.pfs_pb2 import (
    Repo, Commit, Branch, File,
    ListRepoRequest, ListCommitRequest, ListBranchRequest, GlobFileRequest, GetFileRequest,
    CreateRepoRequest, DeleteRepoRequest,
    CreateBranchRequest, DeleteBranchRequest,
    StartCommitRequest, FinishCommitRequest, DeleteCommitRequest,
    PutFileRequest, DeleteFileRequest,
    RESERVED as FILETYPE_RESERVED, FILE as FILETYPE_FILE, DIR as FILETYPE_DIR,
    NONE as DELIMITER_NONE, JSON as DELIMITER_JSON, LINE as DELIMITER_LINE, SQL as DELIMITER_SQL, CSV as DELIMITER_CSV
)
from python_pachyderm.client.pfs.pfs_pb2_grpc import APIStub as PfsAPIStub
from python_pachyderm.client.version.versionpb.version_pb2 import Version
from python_pachyderm.client.version.versionpb.version_pb2_grpc import APIStub as VersionAPIStub


class PachydermException(Exception):

    def __init__(self, message: str, code=None):
        super().__init__(message)
        try:
            self.status_code = code.value[0]
            self.status = code.value[1]
        except (AttributeError, KeyError):
            self.status_code = None
            self.status = None


def retry(channel: str):
    def decorator(f):
        def retry_wrapper(self, *args, **kwargs):
            print(f'Trying ({self.__class__.__name__}.{f.__name__})...')
            adapter = self.adapter if hasattr(self, 'adapter') else self
            try:
                return f(self, *args, **kwargs)
            except grpc._channel._Rendezvous as e:
                if e.code().value[1] == 'unavailable' and adapter._retries < adapter._max_retries:
                    print('Unavailable')
                    if adapter.check_connectivity(channel):
                        adapter._retries += 1
                        return retry_wrapper(self, *args, **kwargs)
                raise PachydermException(e.details(), e.code())
            else:
                print('All good')
                adapter._retries = 0
                adapter._connectable = True
        return retry_wrapper
    return decorator


class PachydermAdapter:

    """Adapter class handling communication with Pachyderm.

    It is effectively a wrapper around the python_pachyderm package.
    """

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None):
        if host is None:
            host = os.getenv('PACHD_ADDRESS') or os.getenv('PACHD_SERVICE_HOST')
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
        if host is None:
            host = 'localhost'
        if port is None:
            port = int(os.getenv('PACHD_SERVICE_PORT', 30650))

        self.host = host
        self.port = port
        self.channels = {
            c: grpc.insecure_channel(f'{host}:{port}')
            for c in ['pfs', 'pps', 'version']
        }
        self.pfs_stub = PfsAPIStub(self.channels['pfs'])
        self.pps_stub = PpsAPIStub(self.channels['pps'])
        self.version_stub = VersionAPIStub(self.channels['version'])

        self._retries = 0
        self._max_retries = 1
        self._connectable: Optional[bool] = None

    def check_connectivity(self, channel: str = 'pfs', timeout: float = 10.0) -> bool:
        """Checks the connectivity to pachd. Tries to connect if not currently connected.

        The gRPC channel connectivity knows 5 states:
        0 = idle, 1 = connecting, 2 = ready, 3 = transient failure, 4 = shutdown.

        Args:
            timeout: Timeout in seconds.

        Returns:
            True if the connectivity state is ready (2), False otherwise.
        """
        print(f'Checking connectivity of {channel} channel...')
        connectivity = 0
        timeout = time.time() + timeout
        connectivity = self.channels[channel]._channel.check_connectivity_state(True)
        while connectivity < 2:
            if time.time() > timeout:
                connectivity = 5
                break
            time.sleep(0.001)
            connectivity = self.channels[channel]._channel.check_connectivity_state(False)
        self._connectable = connectivity == 2
        print('Success!' if self._connectable else 'No connectivity.')
        return self._connectable

    @retry('pfs')
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

    @retry('pfs')
    def list_repo_names(self) -> List[str]:
        repos = self.pfs_stub.ListRepo(ListRepoRequest()).repo_info
        return [r.repo.name for r in repos]

    @retry('pfs')
    def list_branch_heads(self, repo: str) -> Dict[str, str]:
        branches = self.pfs_stub.ListBranch(ListBranchRequest(repo=Repo(name=repo))).branch_info
        return {b.branch.name: b.head.id for b in branches}

    @retry('pfs')
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

    @retry('pfs')
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

    @retry('pps')
    def list_pipelines(self) -> pd.DataFrame:
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
                return str(i.cron.spec)
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
        pipelines = self.pps_stub.ListPipeline(ListPipelineRequest()).pipeline_info
        for pipeline in pipelines:
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

    @retry('pps')
    def list_pipeline_names(self) -> List[str]:
        pipelines = self.pps_stub.ListPipeline(ListPipelineRequest()).pipeline_info
        return [p.pipeline.name for p in pipelines]

    @retry('pps')
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

    @retry('pps')
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

    @retry('pps')
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

    @retry('pps')
    def create_pipeline(self, pipeline_specs: dict) -> None:
        self.pps_stub.CreatePipeline(CreatePipelineRequest(**pipeline_specs))

    @retry('pps')
    def update_pipeline(self, pipeline_specs: dict, reprocess: bool = False) -> None:
        self.pps_stub.CreatePipeline(CreatePipelineRequest(update=True, reprocess=reprocess, **pipeline_specs))

    @retry('pps')
    def delete_pipeline(self, pipeline: str) -> None:
        self.pps_stub.DeletePipeline(DeletePipelineRequest(pipeline=Pipeline(name=pipeline)))

    @retry('pps')
    def start_pipeline(self, pipeline: str) -> None:
        self.pps_stub.StartPipeline(StartPipelineRequest(pipeline=Pipeline(name=pipeline)))

    @retry('pps')
    def stop_pipeline(self, pipeline: str) -> None:
        self.pps_stub.StopPipeline(StopPipelineRequest(pipeline=Pipeline(name=pipeline)))

    @retry('pfs')
    def create_repo(self, repo: str, description: Optional[str] = None) -> None:
        self.pfs_stub.CreateRepo(CreateRepoRequest(repo=Repo(name=repo), description=description))

    @retry('pfs')
    def delete_repo(self, repo: str, force: bool = False) -> None:
        self.pfs_stub.DeleteRepo(DeleteRepoRequest(repo=Repo(name=repo), force=force))

    @retry('pfs')
    def delete_commit(self, repo: str, commit: str) -> None:
        self.pfs_stub.DeleteCommit(DeleteCommitRequest(commit=Commit(repo=Repo(name=repo), id=commit)))

    @retry('pfs')
    def create_branch(self, repo: str, commit: str, branch: str) -> None:
        repo = Repo(name=repo)
        commit = Commit(repo=repo, id=commit)
        branch = Branch(repo=repo, name=branch)
        self.pfs_stub.CreateBranch(CreateBranchRequest(head=commit, branch=branch))

    @retry('pfs')
    def delete_branch(self, repo: str, branch: str) -> None:
        self.pfs_stub.DeleteBranch(DeleteBranchRequest(branch=Branch(repo=Repo(name=repo), name=branch)))

    @retry('pfs')
    def get_file(self, repo: str, path: str, branch: Optional[str] = 'master', commit: Optional[str] = None) -> Generator[bytes, None, None]:
        if commit is None and branch is not None:
            commit = self.list_branch_heads(repo).get(branch)
        file = File(commit=Commit(repo=Repo(name=repo), id=commit), path=path)
        response = self.pfs_stub.GetFile(GetFileRequest(file=file))
        for content in response:
            yield content.value

    @retry('version')
    def get_version(self) -> str:
        version = self.version_stub.GetVersion(Version())
        return f'{version.major}.{version.minor}.{version.micro}'


class PachydermCommitAdapter:

    def __init__(self, adapter: PachydermAdapter, repo: str, branch: Optional[str] = 'master', parent_commit: Optional[str] = None):
        self.adapter = adapter
        self.repo = repo
        self.branch = branch
        self.parent_commit = parent_commit
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

    @retry('pfs')
    def start(self):
        """Start the commit."""
        self._raise_if_finished()
        parent_commit = Commit(repo=Repo(name=self.repo), id=self.parent_commit)
        self.adapter.pfs_stub.StartCommit(StartCommitRequest(parent=parent_commit, branch=self.branch))

    @retry('pfs')
    def finish(self):
        """Finish the commit."""
        self._raise_if_finished()
        self.adapter.pfs_stub.FinishCommit(FinishCommitRequest(commit=self._commit))
        self._finished = True

    @retry('pfs')
    def delete(self):
        """Delete the commit."""
        self.adapter.pfs_stub.DeleteCommit(DeleteCommitRequest(commit=self._commit))
        self._finished = True

    @retry('pfs')
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

    @retry('pfs')
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

    @retry('pfs')
    def delete_file(self, path: str) -> None:
        """Deletes the file found in a given `path` in PFS, if it exists.

        Args:
            path: PFS path of file to delete.
        """
        self._raise_if_finished()
        self.adapter.pfs_stub.DeleteFile(DeleteFileRequest(file=File(commit=self._commit, path=path)))

    @retry('pfs')
    def create_branch(self, branch: str) -> None:
        """Sets this commit as a branch.

        Args:
            branch: Name of the branch.
        """
        branch = Branch(repo=Repo(name=self.repo), name=branch)
        self.adapter.pfs_stub.CreateBranch(CreateBranchRequest(head=self._commit, branch=branch))

    @retry('pfs')
    def _list_file_paths(self, glob: str) -> List[str]:
        return [str(f.file.path) for f in self.adapter.pfs_stub.GlobFileStream(GlobFileRequest(commit=self._commit, pattern=glob))]

    def _raise_if_finished(self):
        if self.finished:
            raise PachydermException(f'Commit {self.commit} is already finished')


def _invert_dict(d: Dict[str, str]) -> Dict[str, List[str]]:
    inverted: Dict[str, List[str]] = {}
    for key, value in d.items():
        inverted.setdefault(value, []).append(key)
    return inverted


def _to_timestamp(seconds: int, nanos: int) -> pd.Timestamp:
    return pd.Timestamp(float(f'{seconds}.{nanos}'), unit='s')


def _to_timedelta(seconds: int, nanos: int) -> pd.Timedelta:
    return pd.Timedelta(float(f'{seconds}.{nanos}'), unit='s')
