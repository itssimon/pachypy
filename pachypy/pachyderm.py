__all__ = [
    'PachydermClient'
]

import os
import json
import yaml
from glob import glob
from fnmatch import fnmatch
from functools import lru_cache
from datetime import datetime

from tzlocal import get_localzone
from termcolor import cprint
import pandas as pd

from python_pachyderm import PpsClient, PfsClient
from python_pachyderm.client.pps.pps_pb2 import ListJobRequest, CreatePipelineRequest, DeletePipelineRequest, Pipeline
from python_pachyderm.pps_client import JOB_STARTING, JOB_RUNNING, JOB_FAILURE, JOB_SUCCESS, JOB_KILLED
from python_pachyderm.pps_client import PIPELINE_STARTING, PIPELINE_RUNNING, PIPELINE_RESTARTING, PIPELINE_FAILURE, PIPELINE_PAUSED, PIPELINE_STANDBY

from .registry import Registry, DockerRegistry, AmazonECRRegistry


STYLE_HIGHLIGHT_CSS = 'color: #d65f5f; font-weight: bold'
STYLE_BAR_COLOR = '#d65f5f44'


class PachydermClientBase:

    def __init__(self, host='localhost', port=30650):
        self.pps_client = PpsClient(host, port)
        self.pfs_client = PfsClient(host, port)

    def list_repo(self):
        res = []
        for repo in self.pfs_client.list_repo():
            res.append({
                'repo': repo.repo.name,
                'size_bytes': repo.size_bytes,
                'branches': len(repo.branches),
                'created': repo.created.seconds,
            })
        return pd.DataFrame(res)

    def list_pipeline(self):
        res = []
        for pipeline in self.pps_client.list_pipeline().pipeline_info:
            res.append({
                'pipeline': pipeline.pipeline.name,
                'parallelism': pipeline.parallelism_spec.constant,
                'created': float(f'{pipeline.created_at.seconds}.{pipeline.created_at.nanos}'),
                'state': pipeline.state,
            })
        return pd.DataFrame(res)

    def list_job(self, pipeline=None, n=20):
        i = 1
        res = []
        for job in self.pps_client.stub.ListJobStream(ListJobRequest(pipeline=Pipeline(name=pipeline))):
            res.append({
                'job': job.job.id,
                'pipeline': job.pipeline.name,
                'state': job.state,
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
            if i > n:
                break
        return pd.DataFrame(res)

    def get_logs(self, pipeline=None, job=None, master=False):
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
        return pd.DataFrame(res)


class PachydermClient(PachydermClientBase):

    def __init__(self, host='localhost', port=30650, container_registry='docker', update_image_digests=True,
                 pipeline_spec_files=None, pipeline_spec_transformer=None, cprint=True):
        super().__init__(host=host, port=port)

        if container_registry == 'docker':
            self.container_registry = DockerRegistry()
        elif container_registry == 'ecr':
            self.container_registry = AmazonECRRegistry()
        elif isinstance(self.container_registry, Registry):
            self.container_registry = container_registry
        elif isinstance(self.container_registry, str):
            self.container_registry = DockerRegistry(container_registry)
        else:
            self.container_registry = None

        self.update_image_digests = update_image_digests
        self.pipeline_spec_files = pipeline_spec_files
        self.pipeline_spec_transformer = pipeline_spec_transformer
        self.cprint = cprint

        try:
            from IPython import get_ipython
            self.notebook_mode = get_ipython().__class__.__name__ == 'ZMQInteractiveShell'
        except:
            self.notebook_mode = False

    def list_repo(self, repos='*', style=True):
        df = super().list_repo()
        if len(df) == 0:
            return None
        if repos is not None and repos != '*':
            df = df[df.repo.apply(lambda p: fnmatch(p, repos))]

        df['is_tick'] = df['repo'].str.endswith('_tick')
        df['created'] = pd.to_datetime(df['created'], unit='s', utc=True).dt.floor('s') \
            .dt.tz_convert(get_localzone().zone).dt.tz_localize(None)
        df['size_megabytes'] = (df['size_bytes'] / 1024.0 / 1024.0).round(1)
        df = df.set_index('repo')[['is_tick', 'branches', 'size_megabytes', 'size_bytes', 'created']].sort_index()

        if self.notebook_mode and style:
            def highlight_true(s):
                return [STYLE_HIGHLIGHT_CSS if v else '' for v in s]

            def highlight_gt1(s):
                return [STYLE_HIGHLIGHT_CSS if v else '' for v in s > 1]

            return df.style \
                .apply(highlight_gt1, subset=['branches']) \
                .apply(highlight_true, subset=['is_tick']) \
                .bar(subset=['size_megabytes'], color=STYLE_BAR_COLOR)
        else:
            return df

    def list_pipeline(self, pipelines='*', style=True):
        df = super().list_pipeline()
        if len(df) == 0:
            return None
        if pipelines is not None and pipelines != '*':
            df = df[df.pipeline.apply(lambda p: fnmatch(p, pipelines))]

        df['parallelism'] = df['parallelism'].fillna(1)
        df['created'] = pd.to_datetime(df['created'], unit='s', utc=True).dt.floor('s') \
            .dt.tz_convert(get_localzone().zone).dt.tz_localize(None)
        df['state'] = df['state'].replace({
            PIPELINE_STARTING: 'starting',
            PIPELINE_RUNNING: 'running',
            PIPELINE_RESTARTING: 'restarting',
            PIPELINE_FAILURE: 'failure',
            PIPELINE_PAUSED: 'paused',
            PIPELINE_STANDBY: 'standby',
        })
        df = df.set_index('pipeline')[['state', 'parallelism', 'created']].sort_index()

        if self.notebook_mode and style:
            def style_state(s):
                color = {'starting': 'orange', 'running': 'green', 'restarting': 'orange', 'failure': 'red', 'paused': 'orange', 'standby': 'blue'}
                return [f'color: {color[v]}; font-weight: bold' if v in color else '' for v in s]

            def highlight_gt1(s):
                return [STYLE_HIGHLIGHT_CSS if v else '' for v in s > 1]

            return df.style \
                .apply(style_state, subset=['state']) \
                .apply(highlight_gt1, subset=['parallelism']) \
                .bar(subset=['parallelism'], color=STYLE_BAR_COLOR)
        else:
            return df

    def list_job(self, pipelines='*', n=20, style=True):
        if pipelines is not None and pipelines != '*':
            pipelines = self._list_pipeline_names(pipelines)
            df = []
            for pipeline in pipelines:
                df.append(super().list_job(pipeline=pipeline, n=n))
            if len(df) > 0:
                df = pd.concat(df)
        else:
            df = super().list_job(n=n)
        if len(df) == 0:
            return None

        for col in ['started', 'finished']:
            df[col] = pd.to_datetime(df[col], unit='s', utc=True).dt.floor('s') \
                .dt.tz_convert(get_localzone().zone).dt.tz_localize(None)
        df = df.reset_index().sort_values(['started', 'index'], ascending=[False, True]).head(n)
        df['date'] = df['started'].dt.date
        df['started'] = df['started'].dt.time
        df['finished'] = df['finished'].dt.time
        df['state'] = df['state'].replace({
            JOB_STARTING: 'starting',
            JOB_RUNNING: 'running',
            JOB_FAILURE: 'failure',
            JOB_SUCCESS: 'success',
            JOB_KILLED: 'killed',
        })
        df = df.set_index('job')[[
            'pipeline', 'state', 'date', 'started', 'finished',
            'data_processed', 'data_skipped', 'data_total',
            'download_bytes', 'upload_bytes',
            'download_time', 'process_time', 'upload_time', 'restart'
        ]]

        if self.notebook_mode and style:
            def style_state(s):
                color = {'starting': 'orange', 'running': 'orange', 'failure': 'red', 'success': 'green', 'killed': 'magenta'}
                return [f'color: {color[v]}; font-weight: bold' if v in color else '' for v in s]

            def highlight_nonzero(s):
                return [STYLE_HIGHLIGHT_CSS if v else '' for v in s != 0]

            def highlight_zero(s):
                return [STYLE_HIGHLIGHT_CSS if v else '' for v in s == 0]

            return df.style \
                .apply(style_state, subset=['state']) \
                .apply(highlight_nonzero, subset=['restart']) \
                .apply(highlight_zero, subset=['data_processed']) \
                .bar(subset=['download_time', 'process_time', 'upload_time'], color=STYLE_BAR_COLOR)
        else:
            return df

    def get_logs(self, pipelines='*', output=True, last_job_only=True, user_only=False, return_df=False):
        logs = []
        for pipeline in self._list_pipeline_names(pipelines):
            logs.append(super().get_logs(pipeline=pipeline))
        if len(logs) == 0:
            self._cprint('No logs found', 'red')
            return None
        df = pd.concat(logs, ignore_index=True).reset_index()
        if len(df) == 0:
            self._cprint('No logs found', 'red')
            return None

        df = df[df['job'].notna()]
        df['user'] = df['user'].fillna(False)
        if user_only:
            df = df[df['user']]
        df['message'] = df['message'].fillna('')
        df['ts'] = pd.to_datetime(df['ts'], unit='s', utc=True).dt.floor('s').dt.tz_convert(get_localzone().zone).dt.tz_localize(None)
        df['worker_ts_min'] = df.groupby(['job', 'worker'])['ts'].transform('min')
        if len(df) == 0:
            return None
        if last_job_only:
            df['job_ts_min'] = df.groupby(['job'])['ts'].transform('min')
            df['job_rank'] = df.groupby(['pipeline'])['job_ts_min'].transform(lambda x: x.rank(method='dense', ascending=False))
            df = df[df['job_rank'] == 1]
        df = df.sort_values(['worker_ts_min', 'job', 'worker', 'ts', 'index'], ascending=True)
        df = df[['ts', 'job', 'pipeline', 'worker', 'user', 'message']].reset_index(drop=True)

        if output:
            job = None
            worker = None
            for _, row in df.iterrows():
                if row.job != job:
                    print()
                    self._cprint(f' Pipeline {row.pipeline} | Job {row.job} ', 'yellow', 'on_grey')
                if row.worker != worker:
                    self._cprint(f' Worker {row.worker} ', 'white', 'on_grey')
                color = 'grey' if row.user else 'blue'
                message = row.message
                if message.startswith('WARNING'):
                    color = 'magenta'
                elif message.startswith('ERROR'):
                    color = 'red'
                self._cprint(f'[{row.ts}] {message}', color)
                job = row.job
                worker = row.worker

        if return_df:
            return df

    def create_or_update_pipelines(self, pipelines='*', pipeline_specs=None, recreate=False, reprocess=False):
        if pipeline_specs is None:
            pipeline_specs = self.read_pipeline_specs(pipelines)
        else:
            if not isinstance(pipeline_specs, list):
                pipeline_specs = [pipeline_specs]
            if isinstance(pipelines, str) and pipelines != '*':
                pipeline_specs = [p for p in pipeline_specs if fnmatch(p['pipeline']['name'], pipelines)]
            elif isinstance(pipelines, list):
                pipeline_specs = [p for p in pipeline_specs if p['pipeline']['name'] in set(pipelines)]

        existing_pipelines = set(self._list_pipeline_names())
        updated_pipelines = []

        if recreate:
            pipeline_names = [pipeline_spec['pipeline']['name'] for pipeline_spec in pipeline_specs]
            self.delete_pipelines(pipeline_names)

        for pipeline_spec in pipeline_specs:
            pipeline = pipeline_spec['pipeline']['name']
            if pipeline in existing_pipelines and not recreate:
                self._cprint(f'Updating pipeline {pipeline}', 'yellow')
                self.pps_client.stub.CreatePipeline(CreatePipelineRequest(update=True, reprocess=reprocess, **pipeline_specs))
            else:
                self._cprint(f'Creating pipeline {pipeline}', 'yellow')
                self.pps_client.stub.CreatePipeline(CreatePipelineRequest(**pipeline_specs))
            updated_pipelines.append(pipeline)

        self._list_pipeline_names.cache_clear()
        return updated_pipelines

    def create_pipelines(self, pipelines='*', pipeline_specs=None, recreate=False):
        return self.create_or_update_pipelines(pipelines=pipelines, pipeline_specs=pipeline_specs, recreate=recreate, reprocess=False)

    def update_pipelines(self, pipelines='*', pipeline_specs=None, recreate=False, reprocess=False):
        return self.create_or_update_pipelines(pipelines=pipelines, pipeline_specs=pipeline_specs, recreate=recreate, reprocess=reprocess)

    def delete_pipelines(self, pipelines):
        pipelines = pipelines if isinstance(pipelines, list) else self._list_pipeline_names(pipelines)
        existing_pipelines = set(self._list_pipeline_names())
        deleted_pipelines = []

        # Delete existing pipelines in reverse order
        for pipeline in pipelines[::-1]:
            if pipeline in existing_pipelines:
                self._cprint(f'Deleting pipeline {pipeline}', 'yellow')
                self.pps_client.stub.DeletePipeline(DeletePipelineRequest(pipeline=Pipeline(name=pipeline)))
                deleted_pipelines.append(pipeline)
            else:
                self._cprint(f'Pipeline {pipeline} does not exist', 'yellow')

        self._list_pipeline_names.cache_clear()
        return deleted_pipelines

    def start_pipelines(self, pipelines):
        pipelines = pipelines if isinstance(pipelines, list) else self._list_pipeline_names(pipelines)
        existing_pipelines = set(self._list_pipeline_names())
        started_pipelines = []

        for pipeline in pipelines:
            if pipeline in existing_pipelines:
                self._cprint(f'Starting pipeline {pipeline}', 'yellow')
                self.pps_client.start_pipeline(pipeline)
                started_pipelines.append(pipeline)
            else:
                self._cprint(f'Pipeline {pipeline} does not exist', 'yellow')

        return started_pipelines

    def stop_pipelines(self, pipelines):
        pipelines = pipelines if isinstance(pipelines, list) else self._list_pipeline_names(pipelines)
        existing_pipelines = set(self._list_pipeline_names())
        stopped_pipelines = []

        for pipeline in pipelines:
            if pipeline in existing_pipelines:
                self._cprint(f'Stopping pipeline {pipeline}', 'yellow')
                self.pps_client.stop_pipeline(pipeline)
                stopped_pipelines.append(pipeline)
            else:
                self._cprint(f'Pipeline {pipeline} does not exist', 'yellow')

        return stopped_pipelines

    def trigger_pipeline(self, pipeline):
        """Trigger a cron-triggered pipeline by writing a time file into its tick repo.

        Args:
            pipeline (str): Name of pipeline to trigger
        """
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        commit = self.pfs_client.start_commit(pipeline + '_tick', branch='master')
        self.pfs_client.delete_file(commit, 'time')
        self.pfs_client.put_file_bytes(commit, 'time', json.dumps(timestamp).encode('utf-8'))
        self.pfs_client.finish_commit(commit)

    def read_pipeline_specs(self, pipelines='*'):
        """Read pipelines specs from files.

        The spec files are defined through the `pipeline_spec_files` property,
        which can be a list of file paths or a valid glob pattern.

        Args:
            pipelines (str, list[str]): Name or list of names of pipelines to read specifications for.
                                        If single name is given, wildcards (*) are supported.
        """
        files = self.pipeline_spec_files
        if isinstance(files, str):
            files = glob(os.path.expanduser(files))
        assert isinstance(files, list)

        # Subset list of files
        files_subset = [f for f in files if fnmatch(os.path.basename(f), pipelines) or pipelines.startswith(os.path.splitext(os.path.basename(f))[0])]
        if len(files_subset) > 0:
            files = files_subset
        self._cprint(f'Reading pipeline specifications from {len(files)} files', 'yellow')

        # Read pipeline specs from files
        pipeline_specs = []
        for file in set(files):
            with open(file, 'r') as f:
                file_content = yaml.safe_load(f)
                if not isinstance(file_content, list):
                    raise TypeError(f'File {os.path.basename(file)} does not contain a list')
                pipeline_specs.extend(file_content)

        # Transform pipeline specs to meet the Pachyderm specification format
        pipeline_specs = self.transform_pipeline_specs(pipeline_specs)

        # Filter pipelines
        if isinstance(pipelines, str) and pipelines != '*':
            pipeline_specs = [p for p in pipeline_specs if fnmatch(p['pipeline']['name'], pipelines)]
        elif isinstance(pipelines, list):
            pipeline_specs = [p for p in pipeline_specs if p['pipeline']['name'] in set(pipelines)]

        if self.update_image_digests:
            assert isinstance(self.container_registry, Registry), 'container_registry must be set in order to update image digests'
            for pipeline in pipeline_specs:
                repository, tag, digest = _split_image_string(pipeline['transform']['image'])
                digest = self.container_registry.get_image_digest(repository, tag)
                if digest is not None:
                    pipeline['transform']['image'] = f'{repository}:{tag}@{digest}'

        self._cprint(f'Matched specification for {len(pipeline_specs)} pipelines', 'green' if len(pipeline_specs) > 0 else 'red')
        return pipeline_specs

    def transform_pipeline_specs(self, pipeline_specs):
        """Applies default transformations on pipeline specs.

        This includes inheritance of the `image` from previous pipelines if not specified.
        Also applies a custom transformer function on each spec if specified via property `pipeline_spec_transformer`.

        Args:
            pipeline_specs (list[dict]): Pipeline specifications to transform.
        """
        previous_image = None
        for pipeline in pipeline_specs:
            if isinstance(pipeline['pipeline'], str):
                pipeline['pipeline'] = {'name': pipeline['pipeline']}
            if 'image' not in pipeline['transform'] and previous_image is not None:
                pipeline['transform']['image'] = previous_image
            if callable(self.pipeline_spec_transformer):
                pipeline = self.pipeline_spec_transformer(pipeline)
            previous_image = pipeline['transform']['image']
        return pipeline_specs

    def clear_cache(self):
        """Clears cache of existing pipelines and image digests.

        Run this if pipelines have been created/deleted or images updated outside of this package."""
        try:
            self._list_pipeline_names.cache_clear()
            self.container_registry.clear_cache()
        except:
            pass

    @lru_cache(maxsize=None)
    def _list_pipeline_names(self, match=None):
        if match is None or match == '*':
            return [p.pipeline.name for p in self.pps_client.list_pipeline().pipeline_info]
        else:
            return [p for p in self._list_pipeline_names() if fnmatch(p, match)]

    def _cprint(self, text, color=None, on_color=None):
        if self.cprint:
            cprint(text, color=color, on_color=on_color)


def _split_image_string(image):
    image_s1 = image.split('@')
    image_s2 = image_s1[0].split(':')
    repository = image_s2[0]
    tag = image_s2[1] if len(image_s2) > 1 else None
    digest = image_s1[1] if len(image_s1) > 1 else None
    return (repository, tag, digest)
