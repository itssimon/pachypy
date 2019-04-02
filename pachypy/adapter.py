import os
import time
import json
from datetime import datetime
from typing import List, Dict, Generator, Union, Optional, TypeVar, cast

import pandas as pd
from grpc._channel import _Rendezvous
from python_pachyderm import PpsClient, PfsClient
from python_pachyderm.client.pps.pps_pb2 import (
    Pipeline, Job,
    ListJobRequest, ListDatumRequest,
    CreatePipelineRequest, DeletePipelineRequest, StartPipelineRequest, StopPipelineRequest
)
from python_pachyderm.client.pfs.pfs_pb2 import (
    Repo, Commit, Branch, File,
    GlobFileRequest, GetFileRequest, ListCommitRequest, CreateBranchRequest, DeleteBranchRequest
)
from python_pachyderm.pps_client import (
    FAILED as DATUM_FAILED, SUCCESS as DATUM_SUCCESS, SKIPPED as DATUM_SKIPPED, STARTING as DATUM_STARTING,
    JOB_STARTING, JOB_RUNNING, JOB_FAILURE, JOB_SUCCESS, JOB_KILLED,
    PIPELINE_STARTING, PIPELINE_RUNNING, PIPELINE_RESTARTING, PIPELINE_FAILURE, PIPELINE_PAUSED, PIPELINE_STANDBY
)
from python_pachyderm.pfs_client import (
    RESERVED as FILETYPE_RESERVED, FILE as FILETYPE_FILE, DIR as FILETYPE_DIR,
    NONE as DELIMITER_NONE, JSON as DELIMITER_JSON, LINE as DELIMITER_LINE, SQL as DELIMITER_SQL, CSV as DELIMITER_CSV
)


T = TypeVar('T')


class PachydermException(Exception):

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
            self._connectable = True
    return cast(T, retry_wrapper)


class PachydermCommitAdapter:

    """Adapter class and context manager for a commit.

    Objects of this class are typically created via :meth:`~pachypy.adapter.PachydermAdapter.commit`.

    Args:
        pfs_client: PFS client object.
        repo: Name of repository.
        branch: Branch in repository. When the commit is started on a branch the previous head of the branch is
                used as the parent of the commit. You may pass None in which case the new commit will have
                no parent (unless parent_commit is specified) and will initially appear empty.
        parent_commit: ID of parent commit. Upon creation the new commit will appear identical to the parent commit.
            Data can safely be added to the new commit without affecting the contents of the parent commit.
    """

    def __init__(self, pfs_client: PfsClient, repo: str, branch: Optional[str] = 'master', parent_commit: Optional[str] = None):
        self.pfs_client = pfs_client
        self.repo = repo
        self.branch = branch
        self.parent_commit = parent_commit
        self.max_retries = 1
        self._commit = None
        self._finished = False
        self._retries = 0

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

    @retry
    def start(self):
        """Start the commit."""
        self._raise_if_finished()
        self._commit = self.pfs_client.start_commit(repo_name=self.repo, branch=self.branch, parent=self.parent_commit)

    @retry
    def finish(self):
        """Finish the commit."""
        self._raise_if_finished()
        self.pfs_client.finish_commit(self._commit)
        self._finished = True

    @retry
    def delete(self):
        """Delete the commit."""
        self.pfs_client.delete_commit(self._commit)
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
        self.pfs_client.put_file_bytes(self._commit, path, value,
                                       delimiter=delimiter, target_file_datums=target_file_datums, target_file_bytes=target_file_bytes)

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
        self.pfs_client.put_file_url(self._commit, path, url, recursive=recursive)

    @retry
    def delete_file(self, path: str) -> None:
        """Deletes the file found in a given `path` in PFS.

        Args:
            path: PFS path of file to delete.
        """
        self._raise_if_finished()
        self.pfs_client.delete_file(self._commit, path)

    @retry
    def create_branch(self, branch: str) -> None:
        """Sets this commit as a branch.

        Args:
            branch: Name of the branch.
        """
        branch = Branch(repo=Repo(name=self.repo), name=branch)
        self.pfs_client.stub.CreateBranch(CreateBranchRequest(head=self._commit, branch=branch))

    @retry
    def list_file_paths(self, glob: str) -> List[str]:
        return [str(f.file.path) for f in self.pfs_client.stub.GlobFileStream(GlobFileRequest(commit=self._commit, pattern=glob))]

    def _raise_if_finished(self):
        if self.finished:
            raise PachydermException(f'Commit {self.commit} is already finished')


class PachydermAdapter:

    """Adapter class handling communication with Pachyderm.

    It is effectively a wrapper around the python_pachyderm package.

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
        self._connectable: Optional[bool] = None

    @property
    def host(self) -> str:
        return str(self.pps_client.channel._channel.target().decode().split(':')[0])

    @property
    def port(self) -> int:
        return int(self.pps_client.channel._channel.target().decode().split(':')[1])

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
        connectivity = self.pfs_client.channel._channel.check_connectivity_state(True)
        while connectivity < 2:
            if time.time() > timeout:
                connectivity = 5
                break
            time.sleep(0.001)
            connectivity = self.pfs_client.channel._channel.check_connectivity_state(False)
        self._connectable = connectivity == 2
        return self._connectable

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
    def list_branch_heads(self, repo: str) -> Dict[str, str]:
        return {b.branch.name: b.head.id for b in self.pfs_client.list_branch(repo)}

    @retry
    def list_commits(self, repo: str, n: int = 20) -> pd.DataFrame:
        """Returns list of commits.

        Args:
            repo: Name of repo to list commits for.
        """
        i = 1
        res = []
        commit_branches = _commit_branches(self.list_branch_heads(repo))
        for commit in self.pfs_client.stub.ListCommitStream(ListCommitRequest(repo=Repo(name=repo))):
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
                break
        return pd.DataFrame(res, columns=['repo', 'commit', 'branches', 'size_bytes', 'started', 'finished', 'parent_commit']) \
            .astype({'size_bytes': 'int', 'started': 'datetime64[ns]', 'finished': 'datetime64[ns]'})

    @retry
    def list_files(self, repo: str, branch: Optional[str] = 'master', commit: Optional[str] = None, glob: str = '**') -> pd.DataFrame:
        """Returns list of files.

        Args:
            repo: Name of repo to list files for.
            branch: Branch of repo to list files for.
            commit: Commit ID to list files for. Overrides `branch` if specified.
            glob: Glob pattern to filter files returned.
        """
        if branch is None and commit is None:
            raise ValueError('branch and commit cannot both be None')
        file_type_mapping = {
            FILETYPE_RESERVED: 'reserved',
            FILETYPE_FILE: 'file',
            FILETYPE_DIR: 'dir',
        }
        res = []
        branch_heads = self.list_branch_heads(repo)
        commit_branches = _commit_branches(branch_heads)
        if commit is None and branch is not None:
            commit = branch_heads.get(branch)
        if commit is not None:
            commit = Commit(repo=Repo(name=repo), id=commit)
            for file in self.pfs_client.stub.GlobFileStream(GlobFileRequest(commit=commit, pattern=glob)):
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
        """Returns list of last `n` jobs.

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
        return pd.DataFrame(res, columns=['job', 'datum', 'state', 'repo', 'path', 'type', 'size_bytes', 'commit', 'committed']) \
            .astype({'size_bytes': 'int', 'committed': 'datetime64[ns]'})

    @retry
    def get_logs(self, pipeline: Optional[str] = None, job: Optional[str] = None, master: bool = False) -> pd.DataFrame:
        """Returns log entries.

        Args:
            pipeline: Name of pipeline to filter logs by.
            job: ID of job to filter logs by.
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
        """Creates pipeline with given specs.

        Args:
            pipeline_specs: Pipeline specs.
        """
        self.pps_client.stub.CreatePipeline(CreatePipelineRequest(**pipeline_specs))

    @retry
    def update_pipeline(self, pipeline_specs: dict, reprocess: bool = False) -> None:
        """Updates existing pipeline with given specs.

        Args:
            pipeline_specs: Pipeline specs.
            reprocess: Whether to reprocess datums with updated pipeline.
        """
        self.pps_client.stub.CreatePipeline(CreatePipelineRequest(update=True, reprocess=reprocess, **pipeline_specs))

    @retry
    def delete_pipeline(self, pipeline: str) -> None:
        """Deletes a pipeline.

        Args:
            pipeline: Name of pipeline to delete.
        """
        self.pps_client.stub.DeletePipeline(DeletePipelineRequest(pipeline=Pipeline(name=pipeline)))

    @retry
    def start_pipeline(self, pipeline: str) -> None:
        """Restarts a stopped pipeline.

        Args:
            pipeline: Name of pipeline to start.
        """
        self.pps_client.stub.StartPipeline(StartPipelineRequest(pipeline=Pipeline(name=pipeline)))

    @retry
    def stop_pipeline(self, pipeline: str) -> None:
        """Stops a running pipeline.

        Args:
            pipeline: Name of pipeline to stop.
        """
        self.pps_client.stub.StopPipeline(StopPipelineRequest(pipeline=Pipeline(name=pipeline)))

    @retry
    def create_repo(self, repo: str, description: Optional[str] = None) -> None:
        """Creates a new repository in PFS.

        Args:
            repo: Name of new repository.
            description: Repository description.
        """
        self.pfs_client.create_repo(repo, description=description)

    @retry
    def delete_repo(self, repo: str) -> None:
        """Delete a repository in PFS.

        Args:
            repo: Name of repository to delete.
        """
        self.pfs_client.delete_repo(repo)

    @retry
    def delete_commit(self, repo: str, commit: str) -> None:
        """Deletes a commit.

        Args:
            repo: Name of repository.
            commit: ID of commit to delete.
        """
        self.pfs_client.delete_commit(Commit(repo=Repo(name=repo), id=commit))

    @retry
    def create_branch(self, repo: str, commit: str, branch: str) -> None:
        """Sets a commit as a branch.

        Args:
            repo: Name of repository.
            commit: ID of commit to set as branch.
            branch: Name of the branch.
        """
        repo = Repo(name=repo)
        commit = Commit(repo=repo, id=commit)
        branch = Branch(repo=repo, name=branch)
        self.pfs_client.stub.CreateBranch(CreateBranchRequest(head=commit, branch=branch))

    @retry
    def delete_branch(self, repo: str, branch: str) -> None:
        """Deletes a branch, but leaves the commits intact.

        The commits can still be accessed via their commit IDs.

        Args:
            repo: Name of repository.
            branch: Name of branch to delete.
        """
        self.pfs_client.stub.DeleteBranch(DeleteBranchRequest(branch=Branch(repo=Repo(name=repo), name=branch)))

    @retry
    def get_file(self, repo: str, path: str, branch: Optional[str] = 'master', commit: Optional[str] = None) -> Generator[bytes, None, None]:
        if commit is None and branch is not None:
            commit = self.list_branch_heads(repo).get(branch)
        response = self.pfs_client.stub.GetFile(GetFileRequest(file=File(commit=Commit(repo=Repo(name=repo), id=commit), path=path)))
        for content in response:
            yield content.value

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


def _commit_branches(branch_heads: Dict[str, str]) -> Dict[str, List[str]]:
    commit_branch: Dict[str, List[str]] = {head: [] for head in branch_heads.values()}
    for branch, head in branch_heads.items():
        commit_branch[head].append(branch)
    return commit_branch


def _to_timestamp(seconds: int, nanos: int) -> pd.Timestamp:
    return pd.Timestamp(float(f'{seconds}.{nanos}'), unit='s')


def _to_timedelta(seconds: int, nanos: int) -> pd.Timedelta:
    return pd.Timedelta(float(f'{seconds}.{nanos}'), unit='s')
