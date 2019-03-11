__all__ = [
    'clear_cache',
    'list_jobs', 'list_pipelines', 'list_repos', 'get_logs',
    'start_pipelines', 'stop_pipelines',
    'read_pipeline_specs', 'create_or_update_pipelines', 'delete_pipelines',
    'trigger_pipeline'
]

import os
import yaml
import json
import subprocess
from tempfile import NamedTemporaryFile
from glob import glob
from fnmatch import fnmatch
from functools import lru_cache
from datetime import datetime

from tzlocal import get_localzone
from termcolor import cprint
import pandas as pd


CONTAINER_REGISTRY = 'index.docker.io'
STYLE_HIGHLIGHT_CSS = 'color: #d65f5f; font-weight: bold'
STYLE_BAR_COLOR = '#d65f5f44'

try:
    from IPython import get_ipython
    NOTEBOOK_MODE = get_ipython().__class__.__name__ == 'ZMQInteractiveShell'
except:
    NOTEBOOK_MODE = False


def clear_cache():
    _list_pipeline_names.cache_clear()
    _get_image_digest_from_ecr.cache_clear()
    _get_image_digest_from_docker_registry.cache_clear()


def list_jobs(pipelines='*', n=20, style=True):
    jq_cmd = "jq -c '{job: .job.id, pipeline: .pipeline.name, state: .state, started: .started, finished: .finished, restart: .restart, data_processed: .data_processed, data_skipped: .data_skipped, data_total: .data_total, download_time: .stats.download_time, process_time: .stats.process_time, upload_time: .stats.upload_time, download_bytes: .stats.download_bytes, upload_bytes: .stats.upload_bytes}'"
    head_cmd = f'head -n {n}'

    if pipelines is not None and pipelines != '*':
        pipelines = _list_pipeline_names(pipelines)
        df = []
        for pipeline in pipelines:
            res = _run_pachctl(f"list-job -p {pipeline} --raw | {jq_cmd} | {head_cmd}")
            df.append(pd.DataFrame(list(map(json.loads, res.splitlines()))))
        if len(df) > 0:
            df = pd.concat(df)
    else:
        res = _run_pachctl(f"list-job --raw | {jq_cmd} | {head_cmd}")
        df = pd.DataFrame(list(map(json.loads, res.splitlines())))

    if len(df) == 0:
        return None

    for col in ['started', 'finished']:
        df[col] = pd.to_datetime(df[col]).dt.floor('s').dt.tz_convert(get_localzone().zone).dt.tz_localize(None)
    df = df.reset_index().sort_values(['started', 'index'], ascending=[False, True]).head(n).set_index('job')
    df['date'] = df['started'].dt.date
    df['started'] = df['started'].dt.time
    df['finished'] = df['finished'].dt.time
    for col in ['data_processed', 'data_skipped', 'data_total', 'restart', 'download_bytes', 'upload_bytes']:
        df[col] = df[col].fillna(0).astype(int)
    for col in ['download_time', 'process_time', 'upload_time']:
        df[col] = df[col].str.strip('s').astype(float).round(2)
    df['state'] = df['state'].replace({
        'JOB_STARTING': 'starting',
        'JOB_RUNNING': 'running',
        'JOB_FAILURE': 'failure',
        'JOB_SUCCESS': 'success',
        'JOB_KILLED': 'killed'
    })
    df = df[[
        'pipeline', 'state', 'date', 'started', 'finished',
        'data_processed', 'data_skipped', 'data_total',
        'download_bytes', 'upload_bytes',
        'download_time', 'process_time', 'upload_time', 'restart'
    ]]

    if NOTEBOOK_MODE and style:
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


def list_pipelines(pipelines='*', style=True):
    res = _run_pachctl("list-pipeline --raw | jq -c '{pipeline: .pipeline.name, parallelism: .parallelismSpec.constant, created: .created_at, state: .state}'")
    df = pd.DataFrame(list(map(json.loads, res.splitlines())))

    if len(df) == 0:
        return None

    if pipelines is not None and pipelines != '*':
        df = df[df.pipeline.apply(lambda p: fnmatch(p, pipelines))]

    df = df.set_index('pipeline')[['state', 'parallelism', 'created']]
    df['parallelism'] = df['parallelism'].fillna(1).astype(int)
    df['created'] = pd.to_datetime(df['created']).dt.floor('s') \
        .dt.tz_convert(get_localzone().zone).dt.tz_localize(None)
    df['state'] = df['state'].replace({
        'PIPELINE_STARTING': 'starting',
        'PIPELINE_RUNNING': 'running',
        'PIPELINE_RESTARTING': 'restarting',
        'PIPELINE_FAILURE': 'failure',
        'PIPELINE_PAUSED': 'paused',
        'PIPELINE_STANDBY': 'standby'
    })

    if NOTEBOOK_MODE and style:
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


def list_repos(repos='*', style=True):
    res = _run_pachctl("list-repo --raw | jq -c '{repo: .repo.name, created: .created, size_bytes: .size_bytes, branches: .branches|length}'")
    df = pd.DataFrame(list(map(json.loads, res.splitlines())))

    if len(df) == 0:
        return None

    if repos is not None and repos != '*':
        df = df[df.repo.apply(lambda p: fnmatch(p, repos))]

    df['is_tick'] = df['repo'].str.endswith('_tick')
    df.set_index('repo', inplace=True)
    df['created'] = pd.to_datetime(df['created']).dt.floor('s') \
        .dt.tz_convert(get_localzone().zone).dt.tz_localize(None)
    df['size_bytes'] = df['size_bytes'].fillna(0).astype(int)
    df['size_mb'] = (df['size_bytes'] / 1024.0 / 1024.0).round(1)
    df = df[['is_tick', 'size_mb', 'size_bytes', 'branches', 'created']].sort_index()

    if NOTEBOOK_MODE and style:
        def highlight_true(s):
            return [STYLE_HIGHLIGHT_CSS if v else '' for v in s]

        def highlight_gt1(s):
            return [STYLE_HIGHLIGHT_CSS if v else '' for v in s > 1]

        return df.style \
            .apply(highlight_gt1, subset=['branches']) \
            .apply(highlight_true, subset=['is_tick']) \
            .bar(subset=['size_mb'], color=STYLE_BAR_COLOR)
    else:
        return df


def get_logs(pipelines='*', output=True, last_job_only=True, user_only=False, return_df=False):
    logs = []

    for pipeline in _list_pipeline_names(pipelines):
        res = _run_pachctl("get-logs -p " + pipeline + " --raw | jq -c '{pipeline: .pipelineName, job: .jobId, worker: .workerId, ts: .ts, user: .user, message: .message}'")
        logs.append(pd.DataFrame(list(map(json.loads, res.splitlines()))))

    if len(logs) == 0:
        return None

    df = pd.concat(logs, ignore_index=True).reset_index()

    if len(df) == 0:
        return None

    df = df[~df['job'].isnull()]
    df['user'] = df['user'].fillna(False)

    if user_only:
        df = df[df['user']]

    df['message'] = df['message'].fillna('')
    df['ts'] = pd.to_datetime(df['ts']).dt.floor('s').dt.tz_convert(get_localzone().zone).dt.tz_localize(None)
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
                cprint(f' Pipeline {row.pipeline} | Job {row.job} ', 'yellow', 'on_grey')
            if row.worker != worker:
                cprint(f' Worker {row.worker} ', 'white', 'on_grey')

            color = 'grey' if row.user else 'blue'
            message = row.message
            if message.startswith('WARNING'):
                color = 'magenta'
            elif message.startswith('ERROR'):
                color = 'red'
            cprint(f'[{row.ts}] {message}', color)

            job = row.job
            worker = row.worker

    if return_df:
        return df


def read_pipeline_specs(files='pipelines/*.yaml', pipelines='*', update_image_digests=True):
    if isinstance(files, str):
        files = glob(os.path.expanduser(files))
    if pipelines is None:
        pipelines = '*'

    assert isinstance(files, list)
    pipeline_specs = []

    # Subset list of files
    files_subset = [f for f in files if fnmatch(os.path.basename(f), pipelines) or pipelines.startswith(os.path.splitext(os.path.basename(f))[0])]
    if len(files_subset) > 0:
        files = files_subset
    cprint(f'Reading pipeline specifications from {len(files)} files', 'yellow')

    # Read pipeline specs from files
    for file in set(files):
        with open(file, 'r') as f:
            file_content = yaml.safe_load(f)
            if not isinstance(file_content, list):
                raise TypeError(f'File {os.path.basename(file)} does not contain a list')
            pipeline_specs.extend(file_content)

    # Transform pipeline specs to meet the Pachyderm specification format
    pipeline_specs = _transform_pipeline_specs(pipeline_specs)

    # Filter pipelines according to pipelines parameter (with wildcards)
    if pipelines != '*':
        pipeline_specs = [p for p in pipeline_specs if fnmatch(p['pipeline']['name'], pipelines)]

    if update_image_digests:
        pipeline_specs = _update_image_digests(pipeline_specs)

    cprint(f'Matched specification for {len(pipeline_specs)} pipelines', 'green' if len(pipeline_specs) > 0 else 'red')
    return pipeline_specs


def create_or_update_pipelines(pipeline_specs, recreate=False):
    pipeline_specs = pipeline_specs if isinstance(pipeline_specs, list) else [pipeline_specs]
    existing_pipelines = set(_list_pipeline_names())

    if recreate:
        pipeline_names = [pipeline_spec['pipeline']['name'] for pipeline_spec in pipeline_specs]
        delete_pipelines(pipeline_names)

    for pipeline_spec in pipeline_specs:
        pipeline_json = json.dumps(pipeline_spec)
        pipeline_name = pipeline_spec['pipeline']['name']
        pipeline_exists = pipeline_name in existing_pipelines

        if pipeline_exists and not recreate:
            cmd = 'update-pipeline'
            cprint(f'Updating pipeline {pipeline_name}', 'yellow')
        else:
            cmd = 'create-pipeline'
            cprint(f'Creating pipeline {pipeline_name}', 'yellow')

        subprocess.run(f'pachctl {cmd}', shell=True, input=pipeline_json, stdout=subprocess.DEVNULL, encoding='utf-8')

    _list_pipeline_names.cache_clear()


def delete_pipelines(pipelines):
    assert pipelines is not None and pipelines != '*', 'specify which pipelines to delete'
    pipelines = pipelines if isinstance(pipelines, list) else _list_pipeline_names(pipelines)
    existing_pipelines = set(_list_pipeline_names())

    # Delete existing pipelines in reverse order
    for pipeline in pipelines[::-1]:
        if pipeline in existing_pipelines:
            cprint(f'Deleting pipeline {pipeline}', 'yellow')
            _run_pachctl(f'delete-pipeline {pipeline}')
        else:
            cprint(f'Pipeline {pipeline} does not exist', 'yellow')

    _list_pipeline_names.cache_clear()


def start_pipelines(pipelines):
    pipelines = pipelines if isinstance(pipelines, list) else _list_pipeline_names(pipelines)
    existing_pipelines = set(_list_pipeline_names())

    for pipeline in pipelines:
        if pipeline in existing_pipelines:
            cprint(f'Starting pipeline {pipeline}', 'yellow')
            _run_pachctl(f'start-pipeline {pipeline}')
        else:
            cprint(f'Pipeline {pipeline} does not exist', 'yellow')


def stop_pipelines(pipelines):
    pipelines = pipelines if isinstance(pipelines, list) else _list_pipeline_names(pipelines)
    existing_pipelines = set(_list_pipeline_names())

    for pipeline in pipelines:
        if pipeline in existing_pipelines:
            cprint(f'Stopping pipeline {pipeline}', 'yellow')
            _run_pachctl(f'stop-pipeline {pipeline}')
        else:
            cprint(f'Pipeline {pipeline} does not exist', 'yellow')


def trigger_pipeline(pipeline):
    with NamedTemporaryFile('w+b') as tmp:
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        tmp.write(json.dumps(timestamp).encode('utf-8'))
        tmp.flush()
        try:
            _run_pachctl(f'put-file {pipeline}_tick master time -o -f {tmp.name}')
            return True
        except subprocess.CalledProcessError as e:
            if 'has no head' in e.stderr:
                commit_id = _run_pachctl(f'start-commit {pipeline}_tick master').strip()
                _run_pachctl(f'put-file {pipeline}_tick master time -o -f {tmp.name}')
                _run_pachctl(f'finish-commit {pipeline}_tick {commit_id}')
                return True
            else:
                cprint(f'Could not trigger pipeline {pipeline}: {e.stderr}', 'red')
                return False


def _split_image_string(image):
    image_s1 = image.split('@')
    image_s2 = image_s1[0].split(':')
    image = image_s2[0]
    tag = image_s2[1] if len(image_s2) > 1 else None
    digest = image_s1[1] if len(image_s1) > 1 else None
    return (image, tag, digest)


def _update_image_digests(pipeline_specs):
    for pipeline in pipeline_specs:
        image, tag, digest = _split_image_string(pipeline['transform']['image'])
        if image.startswith(CONTAINER_REGISTRY):
            repository = image[len(CONTAINER_REGISTRY) + 1:]
            digest = _get_latest_image_digest(repository, tag)
            if digest is not None:
                pipeline['transform']['image'] = f'{image}:{tag}@{digest}'
    return pipeline_specs


def _transform_pipeline_specs(pipeline_specs):
    pipeline_specs = pipeline_specs if isinstance(pipeline_specs, list) else [pipeline_specs]
    previous_image = None
    for pipeline in pipeline_specs:
        if isinstance(pipeline['pipeline'], str):
            pipeline['pipeline'] = {'name': pipeline['pipeline']}
        if 'image' not in pipeline['transform'] and previous_image is not None:
            pipeline['transform']['image'] = previous_image
        previous_image = pipeline['transform']['image']
    return pipeline_specs


def _run_pachctl(cmd):
    if not cmd.startswith('pachctl'):
        cmd = 'pachctl ' + cmd
    res = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
    if res.stderr.strip().endswith('context deadline exceeded'):
        cprint("pachctl couldn't reach Pachyderm", 'red')
        raise subprocess.CalledProcessError(1, cmd, output=res.stdout, stderr=res.stderr)
    if res.stderr.strip().endswith('transport is closing'):
        cprint("pachctl didn't receive a complete response. Please try again!", 'red')
        raise subprocess.CalledProcessError(1, cmd, output=res.stdout, stderr=res.stderr)
    return res.stdout


def _get_latest_image_digest(repository, tag):
    if CONTAINER_REGISTRY.endswith('amazonaws.com'):
        return _get_image_digest_from_ecr(repository, tag)
    else:
        return _get_image_digest_from_docker_registry(repository, tag)


@lru_cache()
def _get_image_digest_from_ecr(repository, tag):
    import boto3
    try:
        ecr = boto3.client('ecr')
        res = ecr.batch_get_image(imageIds=[{'imageTag': tag}], repositoryName=repository)
        return res['images'][0]['imageId']['imageDigest']
    except KeyError:
        cprint(f'Image digest for {repository}:{tag} not found in registry', 'red')
        return None


@lru_cache()
def _get_image_digest_from_docker_registry(repository, tag):
    from dxf import DXF
    from dxf.exceptions import DXFUnauthorizedError
    import dockercloud
    try:
        dxf = DXF('index.docker.io', repo=repository)
        dxf.authenticate(authorization='Basic ' + dockercloud.api.auth.load_from_file(), actions=['pull'])
    except DXFUnauthorizedError:
        cprint(f'Authentication with Docker registry failed. Run `docker login` first.', 'red')
        return None
    try:
        manifest = json.loads(dxf.get_manifest(tag))
        return manifest['config']['digest']
    except DXFUnauthorizedError:
        cprint(f'Image digest for {repository}:{tag} not found in registry', 'red')
        return None


@lru_cache()
def _list_pipeline_names(match=None):
    if match is None or match == '*':
        return _run_pachctl("list-pipeline --raw | jq -r '.pipeline.name'").splitlines()
    else:
        return [p for p in _list_pipeline_names() if fnmatch(p, match)]
