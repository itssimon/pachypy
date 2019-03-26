__all__ = [
    'PachydermClient'
]

import os
import re
import yaml
from glob import glob
from pathlib import Path
from fnmatch import fnmatch
from functools import lru_cache
from datetime import datetime
from collections import namedtuple
from typing import List, Tuple, Set, Iterable, Callable, Union, Optional

import pandas as pd
from tzlocal import get_localzone
from termcolor import cprint

from .adapter import PachydermAdapter
from .registry import DockerRegistryAdapter, AmazonECRAdapter, GCRAdapter


WildcardFilter = Optional[Union[str, Iterable[str]]]
FileGlob = Optional[Union[str, Path, Iterable[str], Iterable[Path]]]
PipelineChanges = namedtuple('PipelineChanges', ['created', 'updated', 'deleted'])


class PachydermClient:

    """Pachyderm client aiming to make interaction with a Pachyderm cluster more efficient and user-friendly.

    Args:
        host: Hostname or IP address to reach pachd. Attempts to get this from PACHD_ADDRESS or ``~/.pachyderm/config.json`` if not set.
        port: Port on which pachd is listening (usually 30650).
        update_image_digests: Whether to update the image field in pipeline specs with the latest image digest
            to force Kubernetes to pull the latest version from the container registry.
        pipeline_spec_files: Glob pattern or list of file paths to pipeline specs in YAML format.
        pipeline_spec_transformer: Function that takes a pipeline spec as dictionary as the only argument
            and returns a transformed pipeline spec.
        cprint: Whether to print colored status messages to stdout when interacting with this class.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        update_image_digests: bool = True,
        pipeline_spec_files: FileGlob = None,
        pipeline_spec_transformer: Optional[Callable[[dict], dict]] = None,
        cprint: bool = True
    ):
        self.adapter = PachydermAdapter(host=host, port=port)
        self.update_image_digests = update_image_digests
        self.pipeline_spec_files = pipeline_spec_files
        self.pipeline_spec_transformer = pipeline_spec_transformer
        self.cprint = cprint
        self._cprint(f'Created client for Pachyderm cluster at {self.adapter.host}:{self.adapter.port}', 'green')

    @property
    def pipeline_spec_files(self) -> Set[str]:
        if self._pipeline_spec_files is None:
            return set()
        else:
            return set.union(*[set(glob(os.path.expanduser(f))) for f in self._pipeline_spec_files])

    @pipeline_spec_files.setter
    def pipeline_spec_files(self, files: FileGlob):
        if isinstance(files, str) or isinstance(files, Path):
            files = [files]
        self._pipeline_spec_files = files

    @property
    def docker_registry_adapter(self):
        if not hasattr(self, '_docker_registry_adapter'):
            self._docker_registry_adapter = DockerRegistryAdapter()
        return self._docker_registry_adapter

    @docker_registry_adapter.setter
    def docker_registry_adapter(self, adapter: DockerRegistryAdapter):
        self._docker_registry_adapter = adapter

    @property
    def ecr_adapter(self):
        if not hasattr(self, '_ecr_adapter'):
            self._ecr_adapter = AmazonECRAdapter()
        return self._ecr_adapter

    @ecr_adapter.setter
    def ecr_adapter(self, adapter: AmazonECRAdapter):
        self._ecr_adapter = adapter

    @property
    def gcr_adapter(self):
        if not hasattr(self, '_gcr_adapter'):
            self._gcr_adapter = GCRAdapter()
        return self._gcr_adapter

    @gcr_adapter.setter
    def gcr_adapter(self, adapter: GCRAdapter):
        self._gcr_adapter = adapter

    def list_repos(self, repos: WildcardFilter = '*') -> pd.DataFrame:
        """Get list of repos as pandas DataFrame.

        Args:
            repos: Name pattern to filter repos returned. Supports shell-style wildcards.
        """
        df = self.adapter.list_repos()
        if repos is not None and repos != '*':
            df = df[df.repo.isin(_wildcard_filter(df.repo, repos))]
        df['is_tick'] = df['repo'].str.endswith('_tick')
        df['created'] = _tz_localize(df['created'])
        return df.set_index('repo')[['is_tick', 'branches', 'size_bytes', 'created']].sort_index()

    def list_pipelines(self, pipelines: WildcardFilter = '*') -> pd.DataFrame:
        """Get list of pipelines as pandas DataFrame.

        Args:
            pipelines: Name pattern to filter pipelines returned. Supports shell-style wildcards.
        """
        df = self.adapter.list_pipelines()
        if pipelines is not None and pipelines != '*':
            df = df[df.pipeline.isin(_wildcard_filter(df.pipeline, pipelines))]
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
            assert len(pipeline_names) > 0, f'No pipelines matching "{pipelines}" were found. Try clear_cache()?'
            df = pd.concat([self.adapter.list_jobs(pipeline=pipeline, n=n) for pipeline in pipeline_names])
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

    def get_logs(self, pipelines: WildcardFilter = '*', last_job_only: bool = True, user_only: bool = False) -> pd.DataFrame:
        """Get logs for jobs.

        Args:
            pipelines: Pattern to filter logs by pipeline name. Supports shell-style wildcards.
            last_job_only: Whether to only show/return logs for the last job of each pipeline.
            user_only: Whether to only show/return logs generated by user code.
        """
        pipeline_names = self._list_pipeline_names(pipelines)
        assert len(pipeline_names) > 0, f'No pipelines matching "{pipelines}" were found. Try clear_cache()?'
        logs = [self.adapter.get_logs(pipeline=pipeline) for pipeline in pipeline_names]
        df = pd.concat(logs, ignore_index=True).reset_index()
        df = df[df['job'].notna()]
        df['user'] = df['user'].fillna(False)
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
        return df[['ts', 'job', 'pipeline', 'worker', 'user', 'message']].reset_index(drop=True)

    def create_pipelines(self, pipelines: WildcardFilter = '*', pipeline_specs: Optional[List[dict]] = None,
                         recreate: bool = False) -> PipelineChanges:
        """Create or recreate pipelines.

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
        """Update or recreate pipelines.

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
        """Delete existing pipelines.

        Args:
            pipelines: Pattern to filter pipelines by name. Supports shell-style wildcards.

        Returns:
            Names of deleted pipelines.
        """
        pipelines = pipelines if isinstance(pipelines, list) else self._list_pipeline_names(pipelines)
        for pipeline in pipelines[::-1]:
            self._cprint(f'Deleting pipeline {pipeline}', 'yellow')
            self.adapter.delete_pipeline(pipeline)
        self._list_pipeline_names.cache_clear()
        return pipelines[::-1]

    def start_pipelines(self, pipelines: WildcardFilter) -> List[str]:
        """Restart stopped pipelines.

        Args:
            pipelines: Pattern to filter pipelines by name. Supports shell-style wildcards.

        Returns:
            Names of started pipelines.
        """
        pipelines = pipelines if isinstance(pipelines, list) else self._list_pipeline_names(pipelines)
        for pipeline in pipelines:
            self._cprint(f'Starting pipeline {pipeline}', 'yellow')
            self.adapter.start_pipeline(pipeline)
        return pipelines

    def stop_pipelines(self, pipelines: WildcardFilter) -> List[str]:
        """Stop running pipelines.

        Args:
            pipelines: Pattern to filter pipelines by name. Supports shell-style wildcards.

        Returns:
            Names of stopped pipelines.
        """
        pipelines = pipelines if isinstance(pipelines, list) else self._list_pipeline_names(pipelines)
        for pipeline in pipelines:
            self._cprint(f'Stopping pipeline {pipeline}', 'yellow')
            self.adapter.stop_pipeline(pipeline)
        return pipelines

    def trigger_pipeline(self, pipeline: str, input_name: str = 'tick') -> None:
        """Trigger a cron-triggered pipeline by committing a timestamp file into its tick repository.

        This simply calls :meth:`~pachypy.adapter.PachydermAdapter.commit_timestamp_file`,
        expecting the cron input repo of the pipeline to have the default name ``<pipeline>_<input_name>``.

        Args:
            pipeline: Name of pipeline to trigger
            input_name: Name of the cron input. Defaults to 'tick'.
        """
        self.adapter.commit_timestamp_file(repo=f'{pipeline}_{input_name}')

    def read_pipeline_specs(self, pipelines: WildcardFilter = '*') -> List[dict]:
        """Read pipelines specs from files.

        The spec files are defined through the `pipeline_spec_files` property,
        which can be a list of file paths or a valid glob pattern.

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
        self._cprint(f'Reading pipeline specifications from {len(files)} files', 'yellow')

        # Read pipeline specs from files
        pipeline_specs = []
        for file in files:
            with open(file, 'r') as f:
                file_content = yaml.safe_load(f)
                if not isinstance(file_content, list):
                    raise TypeError(f'File {os.path.basename(file)} does not contain a list')
                pipeline_specs.extend(file_content)

        # Filter pipelines
        if pipelines != '*':
            def wildcard_match_pipeline_spec(p):
                try:
                    return _wildcard_match(p['pipeline'] if isinstance(p['pipeline'], str) else p['pipeline']['name'])
                except (KeyError, TypeError):
                    return False
            pipeline_specs = [p for p in pipeline_specs if wildcard_match_pipeline_spec(p)]

        # Transform pipeline specs to meet the Pachyderm specification format
        pipeline_specs = self.transform_pipeline_specs(pipeline_specs)

        self._cprint(f'Matched specification for {len(pipeline_specs)} pipelines', 'green' if len(pipeline_specs) > 0 else 'red')
        return pipeline_specs

    def transform_pipeline_specs(self, pipeline_specs: List[dict]) -> List[dict]:
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
                pipeline['transform']['image'] = self.update_image_digest(pipeline['transform']['image'])
            if callable(self.pipeline_spec_transformer):
                pipeline = self.pipeline_spec_transformer(pipeline)
            previous_image = pipeline['transform']['image']
        return pipeline_specs

    def update_image_digest(self, image: str) -> str:
        repository, tag, digest = _split_image_string(image)

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

    def clear_cache(self) -> None:
        """Clears cache of existing pipelines and image digests.

        Run this if pipelines have been created/deleted or images updated outside of this package."""
        try:
            self._list_pipeline_names.cache_clear()
            self.container_registry.clear_cache()
        except AttributeError:
            pass

    def _create_or_update_pipelines(self, pipelines: WildcardFilter = '*', pipeline_specs: List[dict] = None,
                                    update: bool = True, recreate: bool = False, reprocess: bool = False) -> PipelineChanges:
        if pipeline_specs is None:
            pipeline_specs = self.read_pipeline_specs(pipelines)
        elif pipelines != '*':
            pipeline_specs = [p for p in pipeline_specs if _wildcard_match(p['pipeline']['name'], pipelines)]

        existing_pipelines = set(self._list_pipeline_names())
        created_pipelines = []
        updated_pipelines = []
        deleted_pipelines = []

        if recreate:
            pipeline_names = [pipeline_spec['pipeline']['name'] for pipeline_spec in pipeline_specs]
            deleted_pipelines = self.delete_pipelines(pipeline_names)

        for pipeline_spec in pipeline_specs:
            pipeline = pipeline_spec['pipeline']['name']
            if pipeline in existing_pipelines and not recreate:
                if update:
                    self._cprint(f'Updating pipeline {pipeline}', 'yellow')
                    self.adapter.update_pipeline(pipeline_specs, reprocess=reprocess)
                    updated_pipelines.append(pipeline)
                else:
                    self._cprint(f'Pipeline {pipeline} already exists', 'yellow')
            else:
                self._cprint(f'Creating pipeline {pipeline}', 'yellow')
                self.adapter.create_pipeline(pipeline_specs)
                created_pipelines.append(pipeline)

        self._list_pipeline_names.cache_clear()
        return PipelineChanges(created=created_pipelines, updated=updated_pipelines, deleted=deleted_pipelines)

    @lru_cache(maxsize=None)
    def _list_pipeline_names(self, match: WildcardFilter = None) -> List[str]:
        return self.adapter.list_pipeline_names() if match is None else _wildcard_filter(self._list_pipeline_names(), match)

    def _cprint(self, text, color=None, on_color=None):
        if self.cprint:
            cprint(text, color=color, on_color=on_color)


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


def _split_image_string(image: str) -> Tuple[str, str, str]:
    image_s1 = image.split('@')
    image_s2 = image_s1[0].split(':')
    repository = image_s2[0]
    tag = image_s2[1] if len(image_s2) > 1 else None
    digest = image_s1[1] if len(image_s1) > 1 else None
    return (repository, tag, digest)
