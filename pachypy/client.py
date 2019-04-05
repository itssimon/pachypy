__all__ = [
    'PachydermClient', 'PachydermCommit'
]

import io
import json
import logging
import os
import re
from collections import namedtuple
from datetime import datetime
from fnmatch import fnmatch
from glob import glob
from pathlib import Path
from typing import BinaryIO, Callable, IO, Iterable, List, Optional, Tuple, Union

import chardet
import pandas as pd
import yaml
from tzlocal import get_localzone

from .adapter import PachydermAdapter, PachydermCommitAdapter, PachydermException
from .registry import ContainerRegistryAdapter, DockerRegistryAdapter, AmazonECRAdapter, GCRAdapter

WildcardFilter = Optional[Union[str, Iterable[str]]]
FileGlob = Optional[Union[str, Path, Iterable[Union[str, Path]]]]
PipelineChanges = namedtuple('PipelineChanges', ['created', 'updated', 'deleted'])


class PachydermClientException(PachydermException):

    def __init__(self, message):
        super().__init__(message)


class PachydermClient:

    """Pachyderm client.

    Args:
        host: Hostname or IP address to reach pachd. Attempts to get this from PACHD_ADDRESS or ``~/.pachyderm/config.json`` if not set.
        port: Port on which pachd is listening (usually 30650).
        update_image_digests: Whether to update the image field in pipeline specs with the latest image digest
            to force Kubernetes to pull the latest version from the container registry.
        pipeline_spec_files: Glob pattern(s) to pipeline spec files in YAML format.
        pipeline_spec_transformer: Function that takes a pipeline spec as dictionary as the only argument
            and returns a transformed pipeline spec.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        update_image_digests: bool = True,
        pipeline_spec_files: FileGlob = None,
        pipeline_spec_transformer: Optional[Callable[[dict], dict]] = None,
    ):
        self._pipeline_spec_files: FileGlob = None
        self._docker_registry_adapter: Optional[DockerRegistryAdapter] = None
        self._ecr_adapter: Optional[AmazonECRAdapter] = None
        self._gcr_adapter: Optional[GCRAdapter] = None
        self._logger: Optional[logging.Logger] = None

        self.adapter = PachydermAdapter(host=host, port=port)
        self.update_image_digests = update_image_digests
        self.pipeline_spec_files = pipeline_spec_files
        self.pipeline_spec_transformer = pipeline_spec_transformer

        self.logger.debug(f'Created client for Pachyderm cluster at {self.adapter.host}:{self.adapter.port}')

    @property
    def logger(self) -> logging.Logger:
        if self._logger is None:
            self._logger = logging.getLogger('pachypy')
        return self._logger

    @property
    def pipeline_spec_files(self) -> List[str]:
        return _expand_files(self._pipeline_spec_files)

    @pipeline_spec_files.setter
    def pipeline_spec_files(self, files: FileGlob):
        self._pipeline_spec_files = files

    @property
    def docker_registry_adapter(self) -> DockerRegistryAdapter:
        if self._docker_registry_adapter is None:
            self._docker_registry_adapter = DockerRegistryAdapter()
        return self._docker_registry_adapter

    @docker_registry_adapter.setter
    def docker_registry_adapter(self, adapter: DockerRegistryAdapter):
        self._docker_registry_adapter = adapter

    @property
    def ecr_adapter(self) -> AmazonECRAdapter:
        if self._ecr_adapter is None:
            self._ecr_adapter = AmazonECRAdapter()
        return self._ecr_adapter

    @ecr_adapter.setter
    def ecr_adapter(self, adapter: AmazonECRAdapter):
        self._ecr_adapter = adapter

    @property
    def gcr_adapter(self) -> GCRAdapter:
        if self._gcr_adapter is None:
            self._gcr_adapter = GCRAdapter()
        return self._gcr_adapter

    @gcr_adapter.setter
    def gcr_adapter(self, adapter: GCRAdapter):
        self._gcr_adapter = adapter

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
            df = df[df.repo.isin(set(_wildcard_filter(df.repo, repos)))]
        df['is_tick'] = df['repo'].str.endswith('_tick')
        df['created'] = _tz_localize(df['created'])
        return df.set_index('repo')[['is_tick', 'branches', 'size_bytes', 'created']].sort_index()

    def list_commits(self, repos: WildcardFilter, n: int = 10) -> pd.DataFrame:
        """Get list of commits as pandas DataFrame.

        Args:
            repos: Name pattern to filter repos to return commits for. Supports shell-style wildcards.
            n: Maximum number of commits returned per repo.
        """
        repo_names = self._list_repo_names(repos)
        if len(repo_names) == 0:
            raise PachydermClientException(f'No repos matching "{repos}" were found.')
        df = pd.concat([self.adapter.list_commits(repo=repo, n=n) for repo in self._progress(repo_names, unit='repo')])
        return df.set_index(['repo', 'commit'])[['branches', 'size_bytes', 'started', 'finished', 'parent_commit']]

    def list_files(self, repos: WildcardFilter, branch: Optional[str] = 'master', commit: Optional[str] = None,
                   glob: str = '**', files_only: bool = True) -> pd.DataFrame:
        """Get list of files as pandas DataFrame.

        Args:
            repos: Name pattern to filter repos to return files for. Supports shell-style wildcards.
            branch: Branch to list files for. Defaults to 'master'.
            commit: Commit ID to return files for. Overrides `branch` if specified.
                If specified, the repos parameter must only match the repo this commit ID belongs to.
            glob: Glob pattern to filter files returned.
            files_only: Whether to return only files or include directories.
        """
        repo_names = self._list_repo_names(repos)
        if len(repo_names) == 0:
            raise PachydermClientException(f'No repos matching "{repos}" were found.')
        if commit is not None and len(repo_names) > 1:
            raise PachydermClientException(f'More than one repo matches "{repos}", but it must only match the repo of commit ID "{commit}".')
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
        return df.set_index(['repo', 'path'])[['type', 'size_bytes', 'commit', 'branches', 'committed']]

    def list_pipelines(self, pipelines: WildcardFilter = '*') -> pd.DataFrame:
        """Get list of pipelines as pandas DataFrame.

        Args:
            pipelines: Name pattern to filter pipelines returned. Supports shell-style wildcards.
        """
        df = self.adapter.list_pipelines()
        if pipelines is not None and pipelines != '*':
            df = df[df.pipeline.isin(set(_wildcard_filter(df.pipeline, pipelines)))]
        df['created'] = _tz_localize(df['created'])
        return df.set_index('pipeline')[[
            'state', 'cron_spec', 'input', 'input_repos', 'output_branch',
            'parallelism_constant', 'parallelism_coefficient', 'datum_tries', 'max_queue_size',
            'jobs_running', 'jobs_success', 'jobs_failure', 'created'
        ]]

    def list_jobs(self, pipelines: WildcardFilter = '*', n: int = 20) -> pd.DataFrame:
        """Get list of jobs as pandas DataFrame.

        Args:
            pipelines: Pattern to filter jobs by pipeline name. Supports shell-style wildcards.
            n: Maximum number of jobs returned.
        """
        if pipelines is not None and pipelines != '*':
            pipeline_names = self._list_pipeline_names(pipelines)
            if len(pipeline_names) == 0:
                raise PachydermClientException(f'No pipelines matching "{pipelines}" were found.')
            df = pd.concat([self.adapter.list_jobs(pipeline=pipeline, n=n) for pipeline in self._progress(pipeline_names, unit='pipeline')])
        else:
            df = self.adapter.list_jobs(n=n)
        for col in ['started', 'finished']:
            df[col] = _tz_localize(df[col])
        df = df.reset_index().sort_values(['started', 'index'], ascending=[False, True]).head(n)
        df['duration'] = df['finished'] - df['started']
        df.loc[df['state'] == 'running', 'duration'] = datetime.now() - df['started']
        df['progress'] = (df['data_processed'] + df['data_skipped']) / df['data_total']
        return df.set_index('job')[[
            'pipeline', 'state', 'started', 'finished', 'duration',
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
        return df.set_index('datum')[['job', 'state', 'repo', 'path', 'type', 'size_bytes', 'commit', 'committed']].sort_index()

    def get_logs(self, pipelines: WildcardFilter = '*', datum: Optional[str] = None,
                 last_job_only: bool = True, user_only: bool = False, master: bool = False) -> pd.DataFrame:
        """Get logs for jobs.

        Args:
            pipelines: Pattern to filter logs by pipeline name. Supports shell-style wildcards.
            datum: If specified logs are filtered to a datum ID.
            last_job_only: Whether to only show/return logs for the last job of each pipeline.
            user_only: Whether to only return logs generated by user code.
            master: Whether to include logs from the master process.
        """
        pipeline_names = self._list_pipeline_names(pipelines)
        if len(pipeline_names) == 0:
            raise PachydermClientException(f'No pipelines matching "{pipelines}" were found.')
        logs = [self.adapter.get_logs(pipeline=pipeline, master=master) for pipeline in self._progress(pipeline_names, unit='pipeline')]
        df = pd.concat(logs, ignore_index=True).reset_index()
        df = df[df['job'].notna()]
        df['user'] = df['user'].fillna(False)
        if datum is not None:
            df = df[df['datum'] == datum]
        if user_only:
            df = df[df['user']]
        df['message'] = df['message'].fillna('')
        df['ts'] = _tz_localize(df['ts'])
        df['worker_ts_min'] = df.groupby(['job', 'worker'])['ts'].transform('min')
        if last_job_only:
            df['job_ts_min'] = df.groupby(['job'])['ts'].transform('min')
            df['job_rank'] = df.groupby(['pipeline'])['job_ts_min'].transform(lambda x: x.rank(method='dense', ascending=False))
            df = df[df['job_rank'] == 1]
        df = df.sort_values(['worker_ts_min', 'job', 'worker', 'ts', 'index'], ascending=True)
        return df[['ts', 'job', 'pipeline', 'worker', 'datum', 'message', 'user']].reset_index(drop=True)

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

    def put_timestamp_file(self, repo: str, branch: str = 'master', overwrite: bool = True) -> None:
        """Put a timestamp file in a repository to simulate a cron tick.

        This can be used to trigger pipelines with a cron input.

        Args:
            repo: Repository to put timestamp file in.
            branch: Branch in repository.
            overwrite: Whether to overwrite an existing 'time' file (True),
                or to add a new timestamp file (False).
        """
        if overwrite:
            timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            with self.commit(repo, branch=branch) as c:
                c.delete_file('time')
                c.put_file_bytes(json.dumps(timestamp).encode('utf-8'), 'time')
        else:
            raise NotImplementedError

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

    def get_files(self, repo: str, path: str = '/', glob: str = '**', branch: Optional[str] = 'master', commit: Optional[str] = None,
                  destination: Union[str, Path] = '.') -> None:
        """Retrieves multiple files from a repository in PFS and writes them to a local directory.

        Args:
            repo: Repository to retrieve files from.
            path: Path within repository in PFS to retrieve files from.
            glob: Glob pattern to filter files retrieved from `path`. Is interpreted relative to `path`.
            branch: Branch to retrieve files from.
            commit: Commit to retrieve files from. Overrides `branch` if specified.
            destination: Local path to write files to. Must be a directory. Will be created if it doesn't exist.
        """
        path = '/' + path.strip('/')
        glob = path + '/' + glob
        destination = Path(destination).expanduser().resolve()
        files = self.adapter.list_files(repo, branch=branch, commit=commit, glob=glob)
        files = files[files['type'] == 'file']
        for _, row in self._progress(files.iterrows(), unit='file'):
            local_path = destination / os.path.relpath(row['path'], path)
            os.makedirs(local_path.parent, exist_ok=True)
            self.get_file(repo, path=row['path'], commit=row['commit'], destination=local_path)

    def create_pipelines(self, pipelines: WildcardFilter = '*', pipeline_specs: Optional[List[dict]] = None,
                         recreate: bool = False) -> PipelineChanges:
        """Creates or recreates pipelines.

        Args:
            pipelines: Pattern to filter pipeline specs by pipeline name. Supports shell-style wildcards.
            pipeline_specs: Pipeline specifications. Specs are read from files (see property pipeline_spec_files) if not specified.
            recreate: Whether to delete existing pipelines before recreating them.

        Returns:
            Created and deleted pipeline names.
        """
        return self._create_or_update_pipelines(pipelines=pipelines, pipeline_specs=pipeline_specs,
                                                update=False, recreate=recreate, reprocess=False)

    def update_pipelines(self, pipelines: WildcardFilter = '*', pipeline_specs: Optional[List[dict]] = None,
                         recreate: bool = False, reprocess: bool = False) -> PipelineChanges:
        """Updates or recreates pipelines.

        Non-existing pipelines will be created.

        Args:
            pipelines: Pattern to filter pipeline specs by pipeline name. Supports shell-style wildcards.
            pipeline_specs: Pipeline specifications. Specs are read from files (see property pipeline_spec_files) if not specified.
            recreate: Whether to delete existing pipelines before recreating them.
            reprocess: Whether to reprocess datums with updated pipeline.

        Returns:
            Updated, created and deleted pipeline names.
        """
        return self._create_or_update_pipelines(pipelines=pipelines, pipeline_specs=pipeline_specs,
                                                update=True, recreate=recreate, reprocess=reprocess)

    def delete_pipelines(self, pipelines: WildcardFilter) -> List[str]:
        """Deletes existing pipelines.

        Args:
            pipelines: Pattern to filter pipelines by name. Supports shell-style wildcards.

        Returns:
            Names of deleted pipelines.
        """
        pipelines = self._list_pipeline_names(pipelines)
        for pipeline in pipelines[::-1]:
            self.adapter.delete_pipeline(pipeline)
            self.logger.info(f'Deleted pipeline {pipeline}')
        return pipelines[::-1]

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

    def trigger_pipeline(self, pipeline: str) -> None:
        """Triggers a pipeline with a cron input by committing a timestamp file into its cron input repository.

        This simply calls :meth:`~pachypy.client.PachydermClient.put_timestamp_file`.

        Args:
            pipeline: Name of pipeline to trigger.
        """
        cron_specs = self.adapter.get_pipeline_cron_specs(pipeline)
        if len(cron_specs) == 0:
            raise PachydermClientException(f'Cannot trigger pipeline {pipeline} without cron input')
        self.put_timestamp_file(cron_specs[0]['repo'])

    def delete_job(self, job: str) -> None:
        """Deletes a job.

        Args:
            job: ID of job to delete.
        """
        self.adapter.delete_job(job)

    def _read_pipeline_specs(self, pipelines: WildcardFilter = '*') -> List[dict]:
        """Read pipelines specs from files.

        The spec files are defined through the `pipeline_spec_files` property,
        which can be a list of file paths or glob patterns.

        File names are expected to be a prefix of pipeline names defined in them
        or to also match the given `pipelines` pattern.

        Args:
            pipelines: Pattern to filter pipeline specs by pipeline name. Supports shell-style wildcards.
        """
        # Subset list of files
        files = self.pipeline_spec_files
        files_subset = [
            f for f in files
            if _wildcard_match(os.path.basename(f), pipelines) or (isinstance(pipelines, str) and pipelines.startswith(os.path.splitext(os.path.basename(f))[0]))
        ]
        files = files_subset if len(files_subset) > 0 else files

        # Read pipeline specs from files
        pipeline_specs = []
        n = len(files)
        for file in files:
            with open(file, 'r') as f:
                if file.endswith('yaml') or file.endswith('yml'):
                    file_content = yaml.safe_load(f)
                elif file.endswith('json'):
                    file_content = json.load(f)
                else:
                    n -= 1
                    continue
            if isinstance(file_content, dict):
                file_content = [file_content]
            if isinstance(file_content, list) and len(file_content) > 0:
                for i, pipeline_spec in enumerate(file_content):
                    if isinstance(pipeline_spec, dict) and all(k in pipeline_spec for k in ('pipeline', 'transform', 'input')):
                        pipeline_specs.append(pipeline_spec)
                    else:
                        raise ValueError(f"Item {i+1} in file '{file}' does not look like a pipeline specification")
        self.logger.debug(f'Read pipeline specifications from {n} files')

        # Filter pipelines
        if pipelines != '*':
            def wildcard_match_pipeline_spec(p):
                try:
                    return _wildcard_match(p['pipeline'] if isinstance(p['pipeline'], str) else p['pipeline']['name'], pipelines)
                except (KeyError, TypeError):
                    return False
            pipeline_specs = [p for p in pipeline_specs if wildcard_match_pipeline_spec(p)]

        # Transform pipeline specs to meet the Pachyderm specification format
        pipeline_specs = self._transform_pipeline_specs(pipeline_specs)

        if len(pipeline_specs) > 0:
            self.logger.debug(f'Pattern "{pipelines}" matched specification for {len(pipeline_specs)} pipelines')
        else:
            self.logger.warning(f'Pattern "{pipelines}" did not match any pipeline specifications')

        return pipeline_specs

    def _transform_pipeline_specs(self, pipeline_specs: List[dict]) -> List[dict]:
        """Applies default transformations on pipeline specs.

        This includes inheritance of the `image` from previous pipelines if not specified.
        Also applies a custom transformer function on each spec if specified via property `pipeline_spec_transformer`.

        Args:
            pipeline_specs: Pipeline specifications to transform.
        """
        previous_image = None
        for pipeline in pipeline_specs:
            if isinstance(pipeline['pipeline'], str):
                pipeline['pipeline'] = {'name': pipeline['pipeline']}
            if 'image' not in pipeline['transform'] and previous_image is not None:
                pipeline['transform']['image'] = previous_image
            if self.update_image_digests:
                pipeline['transform']['image'] = self._update_image_digest(pipeline['transform']['image'])
            if callable(self.pipeline_spec_transformer):
                pipeline = self.pipeline_spec_transformer(pipeline)
            previous_image = pipeline['transform']['image']
        return pipeline_specs

    def _update_image_digest(self, image: str) -> str:
        """Add or update the latest image digest to/in an image string of format repository:tag@digest.

        Chooses a container registry adapter to retrieve the latest image digest from
        based on the repository name in the image string:

         - Amazon ECR if the repository contains "ecr.*.amazonaws.com"
         - Google Container Registry if the repository contains "gcr.io"
         - Docker Registry otherwise

        Args:
            image: Image string to add/update digest for.

        Returns:
            Updated image string (repository:tag@digest) if the latest digest was retrieved from the registry.
            Returns an unchanged image string if the image was not found in the registry.
        """
        repository, tag, digest = _split_image_string(image)
        registry_adapter: ContainerRegistryAdapter

        if re.match(r'.+\.ecr\..+\.amazonaws\.com/.+', repository):
            registry_adapter = self.ecr_adapter
        elif re.match(r'(^|\w+\.)gcr\.io/.+', repository):
            registry_adapter = self.gcr_adapter
        else:
            registry_adapter = self.docker_registry_adapter

        digest = registry_adapter.get_image_digest(repository, tag)
        if digest is not None:
            image = f'{repository}:{tag}@{digest}'
        return image

    def _create_or_update_pipelines(self, pipelines: WildcardFilter = '*', pipeline_specs: List[dict] = None,
                                    update: bool = True, recreate: bool = False, reprocess: bool = False) -> PipelineChanges:
        if pipeline_specs is None:
            pipeline_specs = self._read_pipeline_specs(pipelines)
        elif pipelines != '*':
            pipeline_specs = [p for p in pipeline_specs if _wildcard_match(p['pipeline']['name'], pipelines)]

        existing_pipelines = set(self._list_pipeline_names())
        created_pipelines: List[str] = []
        updated_pipelines: List[str] = []
        deleted_pipelines: List[str] = []

        if recreate:
            pipeline_names = [pipeline_spec['pipeline']['name'] for pipeline_spec in pipeline_specs]
            deleted_pipelines = self.delete_pipelines(pipeline_names)

        for pipeline_spec in pipeline_specs:
            pipeline = pipeline_spec['pipeline']['name']
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
        return _wildcard_filter(self.adapter.list_pipeline_names(), match)

    def _list_repo_names(self, match: WildcardFilter = None) -> List[str]:
        return _wildcard_filter(self.adapter.list_repo_names(), match)

    @classmethod
    def _progress(cls, x, **kwargs):
        del kwargs
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

    def put_files(self, files: FileGlob, path: str = '/') -> List[str]:
        """Uploads one or multiple files defined by `files` to the given `path` in PFS.

        Directory structure is not maintained. All files are uploaded into the same directory.

        Args:
            files: Glob pattern(s) to files that should be uploaded.
            path: PFS path to upload files to. Must be a directory.
                Will be created if it doesn't exist. Defaults to the root directory.

        Returns:
            Local paths of uploaded files.
        """
        files = _expand_files(files)
        for file in self.client._progress(files, unit='file'):
            file_path = os.path.join(path, os.path.basename(file))
            self.put_file(file, file_path)
        return files

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


def _wildcard_filter(x: Iterable[str], pattern: WildcardFilter) -> List[str]:
    if pattern is None or pattern == '*':
        return list(x)
    else:
        return [i for i in x if _wildcard_match(i, pattern)]


def _wildcard_match(x: str, pattern: WildcardFilter) -> bool:
    if pattern is None or pattern == '*':
        return True
    elif isinstance(pattern, str):
        return fnmatch(x, pattern)
    else:
        return any([_wildcard_match(x, m) for m in pattern])


def _tz_localize(s: pd.Series) -> pd.Series:
    return s.dt.tz_localize('utc').dt.tz_convert(get_localzone().zone).dt.tz_localize(None)


def _split_image_string(image: str) -> Tuple[str, Optional[str], Optional[str]]:
    image_s1 = image.split('@')
    image_s2 = image_s1[0].split(':')
    repository = image_s2[0]
    tag = image_s2[1] if len(image_s2) > 1 else None
    digest = image_s1[1] if len(image_s1) > 1 else None
    return (repository, tag, digest)


def _expand_files(files: FileGlob) -> List[str]:
    if not files:
        return []
    if isinstance(files, str) or isinstance(files, Path):
        files = [files]
    return sorted(list(set.union(*[set(glob(os.path.expanduser(f))) for f in files])))
