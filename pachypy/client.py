__all__ = [
    'PachydermClient', 'PachydermCommit'
]

import io
import json
import logging
import os
import re
import tarfile
import tempfile
from collections import namedtuple
from datetime import datetime, tzinfo
from pathlib import Path
from typing import List, Set, Dict, Iterable, Callable, IO, BinaryIO, Union, Optional, Any

import chardet
import pandas as pd
import pytz
import yaml
from croniter import croniter
from docker import DockerClient
from tzlocal import get_localzone

from .adapter import PachydermAdapter, PachydermCommitAdapter, PachydermError
from .registry import DockerRegistryAdapter, AmazonECRAdapter
from .utils import WildcardFilter, FileGlob, wildcard_filter, wildcard_match, expand_files


PipelineChanges = namedtuple('PipelineChanges', ['created', 'updated', 'deleted'])


class PachydermClientError(PachydermError):

    def __init__(self, message):
        super().__init__(message)


class DockerError(Exception):
    pass


class PachydermClient:

    """Pachyderm client.

    Args:
        host: Hostname or IP address to reach pachd. Attempts to get this from the
            environment variable PACHD_ADDRESS or ``~/.pachyderm/config.json`` if not set.
        port: Port on which pachd is listening. Defaults to 30650.
        add_image_digests: Whether to add a digest to the image field in pipeline specs to
            to force Pachyderm to pull the latest version from the container registry.
        build_images: Whether to build Docker images for pipelines and push them
            to the container registry. Only applies to pipelines that have the `dockerfile`
            or `dockerfile_path` directive set within the `transform` field.
        pipeline_spec_files: Glob pattern(s) to pipeline spec files in YAML or JSON format.
        pipeline_spec_transformer: Function that takes a pipeline spec as dictionary
            as the only argument and returns a transformed pipeline spec.
        pachd_timezone: Timezone of the system that pachd is running on. This determines
            when cron inputs trigger. Defaults to UTC.
        user_timezone: Timezone of the user which is used to localize timestamps.
            Attempts to get this from the environment variable TZ or system settings if not specified.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        add_image_digests: bool = True,
        build_images: bool = True,
        pipeline_spec_files: FileGlob = None,
        pipeline_spec_transformer: Optional[Callable[[dict], dict]] = None,
        pachd_timezone: Union[str, tzinfo] = 'utc',
        user_timezone: Optional[Union[str, tzinfo]] = None
    ):
        self._pipeline_spec_files: FileGlob = None
        self._docker_client: Optional[DockerClient] = None
        self._docker_registry: Optional[DockerRegistryAdapter] = None
        self._amazon_ecr: Optional[AmazonECRAdapter] = None
        self._image_digests: Dict[str, str] = {}
        self._built_images: Set[str] = set()
        self._logger: Optional[logging.Logger] = None
        self._user_timezone: Optional[tzinfo] = None
        self._pachd_timezone: Optional[tzinfo] = None

        self.adapter = PachydermAdapter(host=host, port=port)
        self.add_image_digests = add_image_digests
        self.build_images = build_images
        self.pipeline_spec_files = pipeline_spec_files
        self.pipeline_spec_transformer = pipeline_spec_transformer
        self.pachd_timezone = pachd_timezone
        self.user_timezone = user_timezone

        self.logger.debug(f'Created client for Pachyderm cluster at {self.adapter.host}:{self.adapter.port}')

    @property
    def logger(self) -> logging.Logger:
        if self._logger is None:
            self._logger = logging.getLogger('pachypy')
        return self._logger

    @property
    def pipeline_spec_files(self) -> List[str]:
        return expand_files(self._pipeline_spec_files)

    @pipeline_spec_files.setter
    def pipeline_spec_files(self, files: FileGlob):
        self._pipeline_spec_files = files

    @property
    def docker_client(self) -> DockerClient:
        if self._docker_client is None:
            self._docker_client = DockerClient.from_env()
        return self._docker_client

    @docker_client.setter
    def docker_client(self, docker_client: DockerClient):
        self._docker_client = docker_client
        if self._docker_registry is not None:
            self._docker_registry.docker_client = docker_client
        if self._amazon_ecr is not None:
            self._amazon_ecr.docker_client = docker_client

    @property
    def docker_registry(self) -> DockerRegistryAdapter:
        if self._docker_registry is None:
            self._docker_registry = DockerRegistryAdapter(self.docker_client)
        return self._docker_registry

    @property
    def amazon_ecr(self) -> AmazonECRAdapter:
        if self._amazon_ecr is None:
            self._amazon_ecr = AmazonECRAdapter(self.docker_client)
        return self._amazon_ecr

    @property
    def user_timezone(self) -> tzinfo:
        if self._user_timezone is None:
            tz = os.environ.get('TZ')
            if tz is not None:
                self._user_timezone = pytz.timezone(tz)
            else:
                self._user_timezone = get_localzone()  # type: ignore
        return self._user_timezone

    @user_timezone.setter
    def user_timezone(self, tz: Optional[Union[tzinfo, str]]):
        if isinstance(tz, str):
            self._user_timezone = pytz.timezone(tz)
        else:
            self._user_timezone = tz

    @property
    def pachd_timezone(self):
        return self._pachd_timezone or pytz.utc

    @pachd_timezone.setter
    def pachd_timezone(self, tz: Optional[Union[tzinfo, str]]):
        if isinstance(tz, str):
            self._pachd_timezone = pytz.timezone(tz)
        else:
            self._pachd_timezone = tz

    @property
    def pachd_version(self) -> str:
        return self.adapter.get_version()

    def list_repos(self, repos: WildcardFilter = '*') -> pd.DataFrame:
        """Get list of repos as pandas DataFrame.

        Args:
            repos: Name pattern to filter repos returned. Supports shell-style wildcards.
        """
        df = self.adapter.list_repos()
        if repos is not None and repos != '*':
            df = df[df.repo.isin(set(wildcard_filter(df.repo, repos)))]
        df['is_tick'] = df['repo'].str.endswith('_tick')
        df['created'] = self._localize_timestamp(df['created'])
        df = df.sort_values(['repo']).reset_index(drop=True)
        return df[['repo', 'is_tick', 'branches', 'size_bytes', 'created']].astype({
            'is_tick': 'bool',
        })

    def list_commits(self, repos: WildcardFilter, n: int = 10) -> pd.DataFrame:
        """Get list of commits as pandas DataFrame.

        Args:
            repos: Name pattern to filter repos to return commits for. Supports shell-style wildcards.
            n: Maximum number of commits returned per repo.
        """
        repo_names = self._list_repo_names(repos)
        if len(repo_names) == 0:
            raise PachydermClientError(f'No repos matching "{repos}" were found.')
        df = pd.concat([self.adapter.list_commits(repo=repo, n=n) for repo in self._progress(repo_names, unit='repo')])
        for col in ('started', 'finished'):
            df[col] = self._localize_timestamp(df[col])
        df = df.sort_values(['repo', 'started'], ascending=[True, False]).reset_index(drop=True)
        return df[['repo', 'commit', 'branches', 'size_bytes', 'started', 'finished', 'parent_commit']]

    def list_files(self, repos: WildcardFilter, glob: str = '**', branch: Optional[str] = 'master', commit: Optional[str] = None,
                   files_only: bool = True) -> pd.DataFrame:
        """Get list of files as pandas DataFrame.

        Args:
            repos: Name pattern to filter repos to return files for. Supports shell-style wildcards.
            glob: Glob pattern to filter files returned.
            branch: Branch to list files for. Defaults to 'master'.
            commit: Commit ID to return files for. Overrides `branch` if specified.
                If specified, the repos parameter must only match the repo this commit ID belongs to.
            files_only: Whether to return only files or include directories.
        """
        repo_names = self._list_repo_names(repos)
        if len(repo_names) == 0:
            raise PachydermClientError(f'No repos matching "{repos}" were found.')
        if commit is not None and len(repo_names) > 1:
            raise PachydermClientError(f'More than one repo matches "{repos}", but it must only match the repo of commit ID "{commit}".')
        if branch is None and commit is None:
            df = pd.concat([
                self.adapter.list_files(repo=repo, branch=None, commit=branch_head, glob=glob)
                for repo in self._progress(repo_names, unit='repo') for branch_head in self.adapter.list_branch_heads(repo).values()
            ])
        else:
            df = pd.concat([
                self.adapter.list_files(repo=repo, branch=branch, commit=commit, glob=glob)
                for repo in self._progress(repo_names, unit='repo')
            ])
        if files_only:
            df = df[df['type'] == 'file']
        df['committed'] = self._localize_timestamp(df['committed'])
        df = df.sort_values(['repo', 'path']).reset_index(drop=True)
        return df[['repo', 'path', 'type', 'size_bytes', 'commit', 'branches', 'committed']]

    def list_pipelines(self, pipelines: WildcardFilter = '*') -> pd.DataFrame:
        """Get list of pipelines as pandas DataFrame.

        Args:
            pipelines: Name pattern to filter pipelines returned. Supports shell-style wildcards.
        """
        df = self.adapter.list_pipelines()
        if pipelines is not None and pipelines != '*':
            df = df[df.pipeline.isin(set(wildcard_filter(df.pipeline, pipelines)))]
        df['created'] = self._localize_timestamp(df['created'])
        df['cron_prev_tick'] = df['cron_spec'].apply(lambda cs: self._calc_cron_tick(cs, prev=True))
        df['cron_next_tick'] = df['cron_spec'].apply(lambda cs: self._calc_cron_tick(cs, prev=False))
        df = df.sort_values(['pipeline']).reset_index(drop=True)
        return df[[
            'pipeline', 'state', 'cron_spec', 'cron_prev_tick', 'cron_next_tick', 'input', 'input_repos', 'output_branch',
            'parallelism_constant', 'parallelism_coefficient', 'datum_tries', 'max_queue_size',
            'jobs_running', 'jobs_success', 'jobs_failure', 'created'
        ]].astype({
            'cron_prev_tick': f'datetime64[ns, {self.user_timezone}]',
            'cron_next_tick': f'datetime64[ns, {self.user_timezone}]',
        })

    def list_jobs(self, pipelines: WildcardFilter = '*', n: int = 20, hide_null_jobs: bool = True) -> pd.DataFrame:
        """Get list of jobs as pandas DataFrame.

        Args:
            pipelines: Pattern to filter jobs by pipeline name. Supports shell-style wildcards.
            n: Maximum number of jobs returned.
            hide_null_jobs: If true, empty jobs with no data to process will be filtered out.
        """
        if pipelines is not None and pipelines != '*':
            pipeline_names = self._list_pipeline_names(pipelines)
            if len(pipeline_names) == 0:
                raise PachydermClientError(f'No pipelines matching "{pipelines}" were found.')
            df = pd.concat([self.adapter.list_jobs(pipeline=pipeline, n=n) for pipeline in self._progress(pipeline_names, unit='pipeline')])
        else:
            df = self.adapter.list_jobs(n=n)
        if hide_null_jobs:
            df = df[df['data_total'] > 0]
        for col in ('started', 'finished'):
            df[col] = self._localize_timestamp(df[col])
        df = df.reset_index().sort_values(['started', 'index'], ascending=[False, True]).head(n).reset_index(drop=True)
        df['duration'] = df['finished'] - df['started']
        now = pd.Timestamp('now', tz=self.user_timezone)
        df.loc[df['state'] == 'running', 'duration'] = now - df['started']
        df['progress'] = (df['data_processed'] + df['data_skipped']) / df['data_total']
        return df[[
            'job', 'pipeline', 'state', 'started', 'finished', 'duration',
            'data_processed', 'data_skipped', 'data_total', 'progress', 'restart',
            'download_time', 'process_time', 'upload_time',
            'download_bytes', 'upload_bytes', 'output_commit',
        ]]

    def list_datums(self, job: str) -> pd.DataFrame:
        """Get list of datums for a job as pandas DataFrame.

        Args:
            job: Job ID to return datums for.
        """
        df = self.adapter.list_datums(job)
        df['committed'] = self._localize_timestamp(df['committed'])
        df = df.sort_values(['datum']).reset_index(drop=True)
        return df[[
            'datum', 'job', 'state', 'repo', 'path', 'type', 'size_bytes',
            'commit', 'committed'
        ]]

    def get_logs(self, pipelines: WildcardFilter = '*', datum: Optional[str] = None,
                 last_job_only: bool = True, user_only: bool = False, master: bool = False, tail: int = 0) -> pd.DataFrame:
        """Get logs for jobs.

        Args:
            pipelines: Pattern to filter logs by pipeline name. Supports shell-style wildcards.
            datum: If specified logs are filtered to a datum ID.
            last_job_only: Whether to only show/return logs for the last job of each pipeline.
                Ignored if `tail` is specified.
            user_only: Whether to only return logs generated by user code.
            master: Whether to include logs from the master process.
            tail: Lines of recent logs to retrieve. This is applied before filtering according to `user_only`,
                so less lines may be returned.
        """
        pipeline_names = self._list_pipeline_names(pipelines)
        if len(pipeline_names) == 0:
            raise PachydermClientError(f'No pipelines matching "{pipelines}" were found.')
        logs = [self.adapter.get_logs(pipeline=pipeline, master=master, tail=tail) for pipeline in self._progress(pipeline_names, unit='pipeline')]
        df = pd.concat(logs, ignore_index=True).reset_index()
        df['user'] = df['user'].fillna(False)
        if datum is not None:
            df = df[df['datum'] == datum]
        if user_only:
            df = df[df['user']]
        df['ts'] = self._localize_timestamp(df['ts'])
        df['worker_ts_min'] = df.groupby(['job', 'worker'])['ts'].transform('min')
        if not tail and last_job_only and len(df) > 0:
            last_job = df[~df['job'].eq('')].groupby('job')['ts'].min().sort_values().index[0]
            df = df[df['job'] == last_job]
        df = df.sort_values(['worker_ts_min', 'job', 'worker', 'ts', 'index'], ascending=True).reset_index(drop=True)
        return df[['ts', 'job', 'pipeline', 'worker', 'datum', 'message', 'user']]

    def inspect_repo(self, repo: str) -> Dict[str, Any]:
        """Returns info about a repo.

        Args:
            repo: Name of repo to get info for.
        """
        return self.adapter.inspect_repo(repo=repo)

    def inspect_pipeline(self, pipeline: str) -> Dict[str, Any]:
        """Returns info about a pipeline.

        Args:
            pipeline: Name of pipeline to get info for.
        """
        return self.adapter.inspect_pipeline(pipeline=pipeline)

    def inspect_job(self, job: str) -> Dict[str, Any]:
        """Returns info about a job.

        Args:
            job: ID of job to get info for.
        """
        return self.adapter.inspect_job(job=job)

    def inspect_datum(self, job: str, datum: str) -> Dict[str, Any]:
        """Returns info about a datum.

        Only works if stats tracking is enabled for the pipeline (enable_stats)
        and raises a PachydermError otherwise.

        Args:
            job: ID of job the datum belongs to.
            datum: ID of datum to get info for.
        """
        return self.adapter.inspect_datum(job=job, datum=datum)

    def create_repos(self, repos: Union[str, Iterable[str]]) -> List[str]:
        """Create one or multiple new repositories in pfs.

        Args:
            repos: Name of new repository or iterable of names.

        Returns:
            Created repos.
        """
        if isinstance(repos, str):
            repos = [repos]
        existing_repos = set(self._list_repo_names())
        created_repos = []
        for repo in repos:
            if repo not in existing_repos:
                self.adapter.create_repo(repo=repo)
                created_repos.append(repo)
                self.logger.info(f'Created repo {repo}')
            else:
                self.logger.warning(f'Repo {repo} already exists')
        return created_repos

    def delete_repos(self, repos: WildcardFilter) -> List[str]:
        """Delete repositories.

        Args:
            repos: Pattern to filter repos to delete. Supports shell-style wildcards.

        Returns:
            Deleted repos.
        """
        repos = self._list_repo_names(repos)
        for repo in repos:
            self.adapter.delete_repo(repo)
            self.logger.info(f'Deleted repo {repo}')
        return repos

    def commit(self, repo: str, branch: Optional[str] = 'master', parent_commit: Optional[str] = None, flush: bool = False) -> 'PachydermCommit':
        """Returns a context manager for a new commit.

        The context manager automatically starts and finishes the commit.
        If an exception occurs, the commit is not finished, but deleted.

        Args:
            repo: Name of repository.
            branch: Branch in repository. When the commit is started on a branch, the previous head of the branch is
                    used as the parent of the commit. You may pass `None` in which case the new commit will have
                    no parent (unless `parent_commit` is specified) and will initially appear empty.
            parent_commit: ID of parent commit. Upon creation the new commit will appear identical to the parent commit.
                Data can safely be added to the new commit without affecting the contents of the parent commit.
            flush: If true, blocks until all jobs triggered by this commit have finished
                when the context is exited (only when leaving the `with` statement).

        Returns:
            Commit object allowing operations inside the commit.
        """
        return PachydermCommit(self, repo, branch=branch, parent_commit=parent_commit, flush=flush)

    def delete_commit(self, repo: str, commit: str) -> None:
        """Deletes a commit.

        Args:
            repo: Name of repository.
            commit: ID of commit to delete.
        """
        self.adapter.delete_commit(repo, commit)

    def create_branch(self, repo: str, commit: str, branch: str) -> None:
        """Sets a commit as a branch.

        Args:
            repo: Name of repository.
            commit: ID of commit to set as branch.
            branch: Name of the branch.
        """
        self.adapter.create_branch(repo, commit, branch)

    def delete_branch(self, repo: str, branch: str) -> None:
        """Deletes a branch, but leaves the commits intact.

        The commits can still be accessed via their commit IDs.

        Args:
            repo: Name of repository.
            branch: Name of branch to delete.
        """
        self.adapter.delete_branch(repo, branch)

    def get_file(self, repo: str, path: str, branch: Optional[str] = 'master', commit: Optional[str] = None,
                 destination: Union[str, Path, BinaryIO] = '.') -> None:
        """Retrieves a file from a repository in PFS and writes it to `destination`.

        Args:
            repo: Repository to retrieve file from.
            path: Path within repository in PFS to retrieve file from.
            branch: Branch to retrieve file from.
            commit: Commit to retrieve file from. Overrides `branch` if specified.
            destination: Local path or binary file object to write file to.
                If it is a directory the file's basename will be appended.
        """
        content = self.adapter.get_file(repo, path, branch=branch, commit=commit)
        if isinstance(destination, (str, Path)):
            if os.path.isdir(destination):
                destination = os.path.join(destination, os.path.basename(path))
            with open(destination, 'wb') as f:
                for value in content:
                    f.write(value)
        else:
            for value in content:
                destination.write(value)

    def get_file_content(self, repo: str, path: str, branch: Optional[str] = 'master', commit: Optional[str] = None,
                         encoding: Optional[str] = None) -> Union[bytes, str]:
        """Retrieves a file from a repository in PFS and returns its content.

        Args:
            repo: Repository to retrieve file from.
            path: Path within repository in PFS to retrieve file from.
            branch: Branch to retrieve file from.
            commit: Commit to retrieve file from. Overrides `branch` if specified.
            encoding: If specified, the file content will be returned as a decoded string.
                May be set to 'auto' to try to infer the encoding automatically.

        Returns:
            File contents as bytes, or as string if `encoding` is set.
        """
        with io.BytesIO() as destination:
            self.get_file(repo, path, branch=branch, commit=commit, destination=destination)
            content = destination.getvalue()
        if encoding == 'auto':
            enc = chardet.detect(content)
            encoding = enc['encoding']
            if enc['confidence'] < 0.9:
                raise ValueError('could not reliably detect encoding of file content')
        if encoding is not None:
            return content.decode(encoding)
        return content

    def get_files(self, repo: str, glob: str = '**', branch: Optional[str] = 'master', commit: Optional[str] = None,
                  path: str = '/', destination: Union[str, Path] = '.', ignore_existing: bool = False) -> None:
        """Retrieves multiple files from a repository in PFS and writes them to a local directory.

        Args:
            repo: Repository to retrieve files from.
            glob: Glob pattern to filter files retrieved from `path`. Is interpreted relative to `path`.
            branch: Branch to retrieve files from.
            commit: Commit to retrieve files from. Overrides `branch` if specified.
            path: Path within repository in PFS to retrieve files from.
            destination: Local path to write files to. Must be a directory. Will be created if it doesn't exist.
            ignore_existing: Whether to ignore or overwrite files that already exist locally.
        """
        path = '/' + path.strip('/')
        glob = path + '/' + glob
        destination = Path(destination).expanduser().resolve()
        files = self.adapter.list_files(repo, branch=branch, commit=commit, glob=glob)
        files = files[files['type'] == 'file']
        for _, row in self._progress(files.iterrows(), n=len(files), unit='file'):
            local_path = destination / os.path.relpath(row['path'], path)
            if ignore_existing and local_path.exists():
                continue
            os.makedirs(local_path.parent, exist_ok=True)
            self.get_file(repo, path=row['path'], commit=row['commit'], destination=local_path)
            self.logger.info(f"Downloaded '{os.path.basename(row['path'])}' to '{local_path.parent}'")

    def create_pipelines(self, pipelines: WildcardFilter = '*', pipeline_specs: Optional[List[dict]] = None,
                         recreate: bool = False, build_options: Optional[dict] = None) -> PipelineChanges:
        """Creates or recreates pipelines.

        Args:
            pipelines: Pattern to filter pipeline specs by pipeline name. Supports shell-style wildcards.
            pipeline_specs: Pipeline specifications. These are read from files
                (see property `pipeline_spec_files`) if not specified.
            recreate: Whether to delete existing pipelines before recreating them.
            build_options: Keyword arguments to pass into the Docker `build()` method when building images.
                A useful example is ``dict(nocache=True)``.

        Returns:
            Created and deleted pipeline names.
        """
        return self._create_or_update_pipelines(pipelines=pipelines, pipeline_specs=pipeline_specs,
                                                update=False, recreate=recreate, reprocess=False, build_options=build_options)

    def update_pipelines(self, pipelines: WildcardFilter = '*', pipeline_specs: Optional[List[dict]] = None,
                         recreate: bool = False, reprocess: bool = False, build_options: Optional[dict] = None) -> PipelineChanges:
        """Updates or recreates pipelines.

        Non-existing pipelines will be created.

        Args:
            pipelines: Pattern to filter pipeline specs by pipeline name. Supports shell-style wildcards.
            pipeline_specs: Pipeline specifications. These are read from files
                (see property `pipeline_spec_files`) if not specified.
            recreate: Whether to delete existing pipelines before recreating them.
            reprocess: Whether to reprocess datums with updated pipeline.
            build_options: Keyword arguments to pass into the Docker `build()` method when building images.
                A useful example is ``dict(nocache=True)``.

        Returns:
            Updated, created and deleted pipeline names.
        """
        return self._create_or_update_pipelines(pipelines=pipelines, pipeline_specs=pipeline_specs,
                                                update=True, recreate=recreate, reprocess=reprocess, build_options=build_options)

    def delete_pipelines(self, pipelines: WildcardFilter) -> List[str]:
        """Deletes existing pipelines.

        Args:
            pipelines: Pattern to filter pipelines by name. Supports shell-style wildcards.

        Returns:
            Names of deleted pipelines.
        """
        pipelines = self._list_pipeline_names(pipelines)
        for pipeline in pipelines:
            self.adapter.delete_pipeline(pipeline)
            self.logger.info(f'Deleted pipeline {pipeline}')
        return pipelines

    def start_pipelines(self, pipelines: WildcardFilter) -> List[str]:
        """Restarts stopped pipelines.

        Args:
            pipelines: Pattern to filter pipelines by name. Supports shell-style wildcards.

        Returns:
            Names of started pipelines.
        """
        pipelines = self._list_pipeline_names(pipelines)
        for pipeline in pipelines:
            self.adapter.start_pipeline(pipeline)
            self.logger.info(f'Started pipeline {pipeline}')
        return pipelines

    def stop_pipelines(self, pipelines: WildcardFilter) -> List[str]:
        """Stops running pipelines.

        Args:
            pipelines: Pattern to filter pipelines by name. Supports shell-style wildcards.

        Returns:
            Names of stopped pipelines.
        """
        pipelines = self._list_pipeline_names(pipelines)
        for pipeline in pipelines:
            self.adapter.stop_pipeline(pipeline)
            self.logger.info(f'Stopped pipeline {pipeline}')
        return pipelines

    def run_pipelines(self, pipelines: WildcardFilter) -> List[str]:
        """Rerun the latest jobs for pipelines.

        Args:
            pipelines: Pattern to filter pipelines by name. Supports shell-style wildcards.

        Returns:
            Names of ran pipelines.
        """
        pipelines = self._list_pipeline_names(pipelines)
        for pipeline in pipelines:
            self.adapter.run_pipeline(pipeline)
            self.logger.info(f'Ran pipeline {pipeline}')
        return pipelines

    def trigger_pipeline(self, pipeline: str, flush: bool = False) -> None:
        """Triggers a pipeline with a cron input by committing a timestamp file into its cron input repository.

        Args:
            pipeline: Name of pipeline to trigger.
            flush: If true, blocks until all triggered jobs have finished.
        """
        cron_specs = self.adapter.get_pipeline_cron_specs(pipeline)
        if len(cron_specs) == 0:
            raise PachydermClientError(f'Cannot trigger pipeline {pipeline} without cron input')
        with self.commit(repo=cron_specs[0]['repo'], flush=flush) as c:
            c.put_timestamp_file(overwrite=cron_specs[0]['overwrite'])

    def delete_job(self, job: str) -> None:
        """Deletes a job.

        Args:
            job: ID of job to delete.
        """
        self.adapter.delete_job(job)

    def read_pipeline_specs(self, pipelines: WildcardFilter = '*') -> List[dict]:
        """Read pipelines specifications from YAML or JSON files.

        The spec files are defined through the `pipeline_spec_files` property,
        which can be a list of file paths or glob patterns.

        File names are expected to be a prefix of pipeline names defined in them
        or to also match the given `pipelines` pattern.

        Args:
            pipelines: Pattern to filter pipeline specs by pipeline name. Supports shell-style wildcards.
        """
        files = self.pipeline_spec_files
        files_subset = [
            f for f in files
            if wildcard_match(os.path.basename(f), pipelines) or (isinstance(pipelines, str) and pipelines.startswith(os.path.splitext(os.path.basename(f))[0]))
        ]
        files = files_subset if len(files_subset) > 0 else files

        pipeline_specs = []
        n = len(files)
        for file in files:
            with open(file, 'r') as f:
                if file.lower().endswith('.yaml') or file.lower().endswith('.yml'):
                    file_content = yaml.safe_load(f)
                elif file.lower().endswith('.json'):
                    file_content = json.load(f)
                else:
                    n -= 1
                    continue
            if isinstance(file_content, dict):
                file_content = [file_content]
            if isinstance(file_content, list) and len(file_content) > 0:
                for i, pipeline_spec in enumerate(file_content):
                    if isinstance(pipeline_spec, dict) and all(k in pipeline_spec for k in ('pipeline', 'transform', 'input')):
                        pipeline_spec['_file'] = Path(file)
                        pipeline_specs.append(pipeline_spec)
                    else:
                        raise ValueError(f"Item {i + 1} in file '{file}' does not look like a pipeline specification")
        self.logger.debug(f'Read pipeline specifications from {n} files')

        if pipelines != '*':
            def wildcard_match_pipeline_spec(p):
                try:
                    return wildcard_match(p['pipeline'] if isinstance(p['pipeline'], str) else p['pipeline']['name'], pipelines)
                except (KeyError, TypeError):
                    return False
            pipeline_specs = [p for p in pipeline_specs if wildcard_match_pipeline_spec(p)]

        pipeline_specs = self._transform_pipeline_specs(pipeline_specs)

        if len(pipeline_specs) > 0:
            self.logger.debug(f'Pattern "{pipelines}" matched specification for {len(pipeline_specs)} pipelines')
        else:
            self.logger.warning(f'Pattern "{pipelines}" did not match any pipeline specifications')

        return pipeline_specs

    def clear_cache(self) -> None:
        self._built_images = set()
        self._image_digests = {}

    def _build_image(self, pipeline_spec: dict, build_options: Optional[dict] = None, push: bool = True) -> None:
        """Build and optionally push the Docker image specified in a pipeline specification.

        Args:
            pipeline_spec: Pipeline specification.
            build_options: Keyword arguments to pass into the Docker `build()` method.
            push: Whether to push the image to the registry after building it.
        """
        image = pipeline_spec['transform']['image']
        if image in self._built_images:
            return
        default_build_options = {'rm': True}
        pipeline_build_options = pipeline_spec['transform'].get('docker_build_options', {})
        build_options = {**default_build_options, **pipeline_build_options, **(build_options or {})}

        if 'dockerfile_path' in pipeline_spec['transform'] and 'docker_build_context' not in pipeline_spec['transform']:
            dockerfile_path = pipeline_spec['transform']['dockerfile_path']
            self.logger.info(f"Building Docker image '{image}' from '{dockerfile_path}' ...")
            build_stream = self.docker_client.api.build(path=str(dockerfile_path), tag=image, decode=True, **build_options)
        elif 'dockerfile' in pipeline_spec['transform'] or 'dockerfile_path' in pipeline_spec['transform']:
            with tempfile.SpooledTemporaryFile(max_size=1000 ** 2) as tmp:
                with tarfile.open(fileobj=tmp, mode='w') as tar:
                    if 'dockerfile' in pipeline_spec['transform']:
                        dockerfile = io.BytesIO(pipeline_spec['transform']['dockerfile'].encode('utf-8'))
                        dockerfile_info = tarfile.TarInfo(name='Dockerfile')
                        dockerfile_info.size = len(dockerfile.getvalue())
                        tar.addfile(dockerfile_info, fileobj=dockerfile)
                    else:
                        tar.add(pipeline_spec['transform']['dockerfile_path'] / 'Dockerfile', arcname='Dockerfile', recursive=False)
                    if 'docker_build_context' in pipeline_spec['transform']:
                        for source_path, context_path in pipeline_spec['transform']['docker_build_context']:
                            tar.add(source_path, arcname=context_path)
                tmp.flush()
                tmp.seek(0)
                self.logger.info(f"Building Docker image '{image}' ...")
                build_stream = self.docker_client.api.build(fileobj=tmp, tag=image, decode=True, custom_context=True, **build_options)
        else:
            return

        for chunk in build_stream:
            if 'error' in chunk:
                raise DockerError(chunk['error'])
            if 'stream' in chunk:
                msg = chunk['stream'].strip()
                if msg:
                    self.logger.debug(msg)
        self._built_images.add(image)

        if push:
            registry_adapter = self._get_registry_adapter(image)
            digest = registry_adapter.push_image(image)
            self._image_digests[image] = digest
            self.logger.info(f"Successfully pushed image '{image}' to registry")
            self.logger.debug(f"Image digest is '{digest}'")

    def _transform_pipeline_specs(self, pipeline_specs: List[dict]) -> List[dict]:
        """Applies default transformations on pipeline specs.

        This includes inheritance of the `image` from previous pipelines if not specified.
        Also applies a custom transformer function on each spec if specified via property `pipeline_spec_transformer`.

        Args:
            pipeline_specs: Pipeline specifications to transform.
        """
        for pipeline in pipeline_specs:
            if callable(self.pipeline_spec_transformer):
                pipeline = self.pipeline_spec_transformer(pipeline)
            if isinstance(pipeline['pipeline'], str):
                pipeline['pipeline'] = {'name': pipeline['pipeline']}
            if 'image' not in pipeline['transform']:
                raise ValueError(f"Pipeline '{pipeline['pipeline']['name']}' is missing the `transform.image` field")
            if 'dockerfile_path' in pipeline['transform']:
                path = Path(pipeline['transform']['dockerfile_path'])
                if not path.is_absolute():
                    path = pipeline['_file'].parent / path
                if not path.is_dir() and path.name.lower() == 'dockerfile':
                    path = path.parent
                pipeline['transform']['dockerfile_path'] = path.resolve()
            if 'docker_build_context' in pipeline['transform']:
                paths = []
                for p in pipeline['transform']['docker_build_context']:
                    if ':' in p:
                        source_path, context_path = p.split(':', 1)
                        source_path = Path(source_path)
                    else:
                        source_path = Path(p)
                        context_path = source_path.name
                    if not source_path.is_absolute():
                        source_path = pipeline['_file'].parent / source_path
                    paths.append((source_path.resolve(), context_path))
                pipeline['transform']['docker_build_context'] = paths
        return pipeline_specs

    def _get_registry_adapter(self, image: str) -> DockerRegistryAdapter:
        """Returns a registry adapter chosen based on an image string.

         - Amazon ECR if the repository contains "ecr.*.amazonaws.com"
         - Docker Registry otherwise

        Args:
            image: Image string.
        """
        if re.match(r'.+\.ecr\..+\.amazonaws\.com/.+', image):
            return self.amazon_ecr
        else:
            return self.docker_registry

    def _add_image_digest(self, image: str) -> str:
        """Adds the latest image digest to an image string.

        Args:
            image: Image string to add digest to.

        Returns:
            Updated image string (repository:tag@digest) if the latest digest was retrieved from the registry.
            Returns an unchanged image string if the image was not found in the registry or
            the image string already contained a digest.
        """
        if '@' in image:
            return image
        if image in self._image_digests:
            digest = self._image_digests[image]
        else:
            registry_adapter = self._get_registry_adapter(image)
            digest = registry_adapter.get_image_digest(image)
        if digest:
            self._image_digests[image] = digest
            return image + '@' + digest
        return image

    def _create_or_update_pipelines(self, pipelines: WildcardFilter = '*', pipeline_specs: List[dict] = None,
                                    update: bool = True, recreate: bool = False, reprocess: bool = False,
                                    build_options: Optional[dict] = None) -> PipelineChanges:
        if pipeline_specs is None:
            pipeline_specs = self.read_pipeline_specs(pipelines)
        elif pipelines != '*':
            pipeline_specs = [p for p in pipeline_specs if wildcard_match(p['pipeline']['name'], pipelines)]

        self.clear_cache()
        existing_pipelines = set(self._list_pipeline_names())
        created_pipelines: List[str] = []
        updated_pipelines: List[str] = []
        deleted_pipelines: List[str] = []

        if recreate:
            pipeline_names = [pipeline_spec['pipeline']['name'] for pipeline_spec in pipeline_specs]
            deleted_pipelines = self.delete_pipelines(pipeline_names)

        for pipeline_spec in self._progress(pipeline_specs, unit='pipeline'):
            pipeline = pipeline_spec['pipeline']['name']
            if self.build_images:
                self._build_image(pipeline_spec, build_options=build_options)
            if self.add_image_digests:
                pipeline_spec['transform']['image'] = self._add_image_digest(pipeline_spec['transform']['image'])
            if pipeline in existing_pipelines and not recreate:
                if update:
                    self.adapter.update_pipeline(pipeline_spec, reprocess=reprocess)
                    updated_pipelines.append(pipeline)
                    self.logger.info(f'Updated pipeline {pipeline}')
                else:
                    self.logger.warning(f'Pipeline {pipeline} already exists')
            else:
                self.adapter.create_pipeline(pipeline_spec)
                created_pipelines.append(pipeline)
                self.logger.info(f'Created pipeline {pipeline}')

        return PipelineChanges(created=created_pipelines, updated=updated_pipelines, deleted=deleted_pipelines)

    def _list_pipeline_names(self, match: WildcardFilter = None) -> List[str]:
        return wildcard_filter(self.adapter.list_pipeline_names(), match)

    def _list_repo_names(self, match: WildcardFilter = None) -> List[str]:
        return wildcard_filter(self.adapter.list_repo_names(), match)

    def _calc_cron_tick(self, cron_spec: str, prev: bool = True) -> Optional[pd.Timestamp]:
        if pd.isna(cron_spec) or cron_spec == '':
            return None
        now = datetime.now(self.pachd_timezone)
        cron = croniter(cron_spec, now)
        tick = cron.get_prev(datetime) if prev else cron.get_next(datetime)
        return pd.Timestamp(tick.astimezone(self.user_timezone))

    def _localize_timestamp(self, ts: pd.Series) -> pd.Series:
        return ts.dt.tz_convert(self.user_timezone)

    @classmethod
    def _progress(cls, x, n=None, **kwargs):
        del n, kwargs
        return x

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.adapter.host}:{self.adapter.port}')"


class PachydermCommit(PachydermCommitAdapter):

    """Represents a commit in Pachyderm.

    Objects of this class are typically created via :meth:`~pachypy.client.PachydermClient.commit`
    and used as a context manager, which automatically starts and finishes a commit.
    """

    def __init__(self, client: PachydermClient, repo: str, branch: Optional[str] = 'master', parent_commit: Optional[str] = None, flush: bool = False):
        self.client = client
        super().__init__(client.adapter, repo, branch=branch, parent_commit=parent_commit, flush=flush)

    def put_file(self, file: Union[str, Path, IO], path: Optional[str] = None,
                 delimiter: str = 'none', target_file_datums: int = 0, target_file_bytes: int = 0) -> None:
        """Uploads a file or the content of a file-like to the given `path` in PFS.

        Args:
            file: A local file path or a file-like object.
            path: PFS path to upload file to. Defaults to the root directory.
                If `path` ends with a slash (/), the basename of `file` will be appended.
                If `file` is a file-like object, `path` needs to be the full PFS path including filename.
            delimiter: Causes data to be broken up into separate files with `path` as a prefix.
                Possible values are 'none' (default), 'json', 'line', 'sql' and 'csv'.
            target_file_datum: Specifies the target number of datums in each written file.
                It may be lower if data does not split evenly, but will never be higher, unless the value is 0 (default).
            target_file_bytes: Specifies the target number of bytes in each written file.
                Files may have more or fewer bytes than the target.
        """
        if isinstance(file, (str, Path)):
            if path is None:
                path = '/'
            if path.endswith('/'):
                path = path + os.path.basename(file)
            with open(Path(file).expanduser().resolve(), 'rb') as f:
                self.put_file_bytes(f.read(), path, delimiter=delimiter, target_file_datums=target_file_datums, target_file_bytes=target_file_bytes)
        else:
            if path is None or path.endswith('/'):
                raise ValueError('path needs to be specified (including filename)')
            self.put_file_bytes(file.read(), path, delimiter=delimiter, target_file_datums=target_file_datums, target_file_bytes=target_file_bytes)  # type: ignore

    def put_files(self, files: FileGlob, path: str = '/', keep_structure: bool = False, base_path: Union[str, Path] = '.') -> List[str]:
        """Uploads one or multiple files defined by `files` to the given `path` in PFS.

        Args:
            files: Glob pattern(s) to files that should be uploaded.
            path: PFS path to upload files to. Must be a directory.
                Will be created if it doesn't exist. Defaults to the root directory.
            keep_structure: If true, the local directory structure is recreated in PFS.
                Use in conjunction with `base_path`.
            base_path: If `keep_structure` is true, PFS paths will be constructed
                relative to `path` and using the local file structure relative to `base_path`.
                This defaults to the current working directory.

        Returns:
            Local paths of uploaded files.
        """
        files = expand_files(files)
        for file in self.client._progress(files, unit='file'):
            if keep_structure:
                file_path = os.path.join(path, os.path.relpath(file, base_path))
            else:
                file_path = os.path.join(path, os.path.basename(file))
            if '../' in file_path:
                raise ValueError(f"'{file_path}' is not a valid path in PFS. You may want to adjust the `base_path` argument.")
            self.put_file(file, file_path)
        return files

    def put_timestamp_file(self, overwrite: bool = False) -> None:
        """Uploads a timestamp file to simulate a cron tick.

        Args:
            overwrite: Whether to overwrite existing timestamp files (True) or
                to just add a new timestamp file (False). Only applies to pachd >=1.8.6.
        """
        if self.client.pachd_version >= '1.8.6':
            if overwrite:
                self.delete_file('/time')
                self.delete_files('/????-??-??T??:??:??*')
            timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
            self.put_file_bytes(b'', timestamp)
        else:
            self.delete_file('/time')
            timestamp = datetime.utcnow().isoformat()[:-3] + 'Z'
            self.put_file_bytes(json.dumps(timestamp).encode('utf-8'), '/time')

    def delete_files(self, glob: str) -> List[str]:
        """Deletes all files matching `glob` in the current commit.

        Args:
            glob: Glob pattern to filter files to delete.

        Returns:
            PFS paths of deleted files.
        """
        files = self._list_file_paths(glob)
        for path in files:
            self.delete_file(path)
        return files

    def __str__(self) -> str:
        return self.commit or ''

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.commit or ''}')"
