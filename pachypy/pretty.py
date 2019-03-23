__all__ = [
    'PrettyPachydermClient'
]

from typing import Dict, List, Iterable, Union, Callable
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from IPython.core.display import HTML
import pandas.io.formats.style as style
import pandas as pd
import numpy as np
from termcolor import cprint

from .client import PachydermClient


FONT_AWESOME = '<link rel="stylesheet" href="https://use.fontawesome.com/releases/v5.8.1/css/all.css" crossorigin="anonymous">'
BAR_COLOR = '#105ecd33'
PROGRESS_BAR_COLOR = '#03820333'


def use_font_awesome(f: Callable):
    def use_font_awesome_wrapper(self, *args, **kwargs):
        ret = f(self, *args, **kwargs)
        return HTML(FONT_AWESOME + ret.render())

    return use_font_awesome_wrapper


class PrettyPachydermClient(PachydermClient):

    @use_font_awesome
    def list_repos(self, repos: str = '*') -> style.Styler:
        df = super().list_repos(repos=repos).reset_index()
        df.rename({
            'repo': 'Repo',
            'is_tick': 'Tick',
            'branches': 'Branches',
            'size_bytes': 'Size',
            'created': 'Created',
        }, axis=1, inplace=True)
        df['Tick'] = df['Tick'].map({True: _fa('stopwatch'), False: ''})
        df['Branches'] = df['Branches'].apply(', '.join)
        return df[['Repo', 'Tick', 'Branches', 'Size', 'Created']].style \
            .bar(subset=['Size'], color=BAR_COLOR) \
            .format({'Created': _format_datetime, 'Size': _format_size}) \
            .set_properties(**{'text-align': 'left'}) \
            .set_properties(subset=['Size', 'Created'], **{'white-space': 'nowrap'}) \
            .set_table_styles([dict(selector='th', props=[('text-align', 'left'), ('white-space', 'nowrap')])]) \
            .hide_index()

    @use_font_awesome
    def list_pipelines(self, pipelines: str = '*') -> style.Styler:
        df = super().list_pipelines(pipelines=pipelines)
        df['sort_key'] = df.index.map(_pipeline_sort_key(df['input_repos'].to_dict()))
        df.reset_index(inplace=True)
        df.sort_values('sort_key', inplace=True)
        df.rename({
            'pipeline': 'Pipeline',
            'state': 'State',
            'cron_spec': 'Cron',
            'input': 'Input',
            'output_branch': 'Output',
            'datum_tries': 'Tries',
            'created': 'Created',
        }, axis=1, inplace=True)
        df.loc[df['jobs_running'] > 0, 'State'] = 'job running'
        df.loc[df['Cron'] != '', 'Cron'] = _fa('stopwatch') + df['Cron']
        df['Parallelism'] = ''
        df.loc[df['parallelism_constant'] > 0, 'Parallelism'] = \
            _fa('hashtag') + df['parallelism_constant'].astype(str)
        df.loc[df['parallelism_coefficient'] > 0, 'Parallelism'] = \
            _fa('asterisk') + df['parallelism_coefficient'].astype(str)
        df['Jobs'] = \
            '<span style="color: green">' + df['jobs_success'].astype(str) + '</span>' + \
            np.where(df['jobs_failure'] > 0, ' + <span style="color: red">' + df['jobs_failure'].astype(str) + '</span>', '')
        return df[['Pipeline', 'State', 'Cron', 'Input', 'Output', 'Tries', 'Parallelism', 'Jobs', 'Created']].style \
            .apply(_style_pipeline_state, subset=['State']) \
            .format({'State': _format_pipeline_state, 'Created': _format_datetime}) \
            .set_properties(**{'text-align': 'left'}) \
            .set_properties(subset=['State', 'Cron', 'Jobs', 'Created'], **{'white-space': 'nowrap'}) \
            .set_table_styles([dict(selector='th', props=[('text-align', 'left')])]) \
            .hide_index()

    @use_font_awesome
    def list_jobs(self, pipelines: str = '*', n: int = 20) -> style.Styler:
        df = super().list_jobs(pipelines=pipelines, n=n).reset_index()
        df.rename({
            'job': 'Job',
            'pipeline': 'Pipeline',
            'state': 'State',
            'started': 'Started',
            'duration': 'Duration',
            'restart': 'Restarts',
            'download_bytes': 'Downloaded',
            'upload_bytes': 'Uploaded',
            'download_time': 'Download Time',
            'process_time': 'Process Time',
            'upload_time': 'Upload Time',
        }, axis=1, inplace=True)
        df['Duration'] = df['Duration'].dt.total_seconds()
        df['Progress'] = \
            df['progress'].apply(lambda x: f'{x:.0%}') + ' | ' + \
            '<span style="color: green">' + df['data_processed'].astype(str) + '</span>' + \
            np.where(df['data_skipped'] > 0, ' + <span style="color: purple">' + df['data_skipped'].astype(str) + '</span>', '') + \
            ' / <span>' + df['data_total'].astype(str) + '</span>'
        return df[['Job', 'Pipeline', 'State', 'Started', 'Duration', 'Progress', 'Restarts', 'Downloaded', 'Uploaded']].style \
            .bar(subset=['Duration'], color=BAR_COLOR) \
            .apply(_style_job_state, subset=['State']) \
            .apply(_style_job_progress, subset=['Progress']) \
            .format({
                'State': _format_job_state,
                'Started': _format_datetime,
                'Duration': _format_duration,
                'Restarts': lambda i: _fa('undo') + str(i) if i > 0 else '',
                'Downloaded': _format_size,
                'Uploaded': _format_size
            }) \
            .set_properties(**{'text-align': 'left'}) \
            .set_properties(subset=['Started', 'Duration', 'Progress'], **{'white-space': 'nowrap'}) \
            .set_table_styles([dict(selector='th', props=[('text-align', 'left')])]) \
            .hide_index()

    def get_logs(self, pipelines: str = '*', last_job_only: bool = True, user_only: bool = False) -> None:
        df = super().get_logs(pipelines=pipelines, last_job_only=last_job_only, user_only=user_only)
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


def _pipeline_sort_key(input_repos: Dict[str, List[str]]):
    def get_dag_distance(p, i=0):
        yield i
        for d in input_repos[p]:
            if d in pipelines:
                yield from get_dag_distance(d, i + 1)

    def get_dag_dependencies(p):
        yield p
        for d in input_repos[p]:
            if d in pipelines:
                yield from get_dag_dependencies(d)

    pipelines = set(input_repos.keys())
    dag_distance = {p: max(list(get_dag_distance(p))) for p in pipelines}
    dag_nodes = {p: set(get_dag_dependencies(p)) for p in pipelines}
    for p, nodes in dag_nodes.items():
        for node in nodes:
            dag_nodes[node].update(nodes)
    dag_name = {p: min(nodes) for p, nodes in dag_nodes.items()}
    return {p: f'{dag_name[p]}/{dag_distance[p]}' for p in pipelines}


def _fa(i: str) -> str:
    return f'<i class="fas fa-fw fa-{i}"></i>&nbsp;'


def _style_pipeline_state(s: Iterable[str]) -> List[str]:
    color = {
        'starting': 'orange',
        'restarting': 'orange',
        'running': 'green',
        'job running': 'purple',
        'failure': 'red',
        'paused': 'orange',
        'standby': '#0251c9',
    }
    return ['color: {c}; font-weight: bold'.format(c=color.get(v, 'gray')) for v in s]


def _style_job_state(s: Iterable[str]) -> List[str]:
    color = {
        'starting': 'orange',
        'running': 'orange',
        'success': 'green',
        'failure': 'red',
        'killed': 'red',
    }
    return ['color: {c}; font-weight: bold'.format(c=color.get(v, 'gray')) for v in s]


def _style_job_progress(s: Iterable[str]) -> List[str]:
    s = s.apply(lambda x: float(x.split('%')[0]))

    def css_bar(end):
        css = 'width: 10em; height: 80%;'
        if end > 0:
            css += 'background: linear-gradient(90deg,'
            css += '{c} {e:.1f}%, transparent {e:.1f}%)'.format(e=min(end, 100), c=PROGRESS_BAR_COLOR)
        return css

    return [css_bar(x) if not pd.isna(x) and x < 100 else '' for x in s]


def _format_pipeline_state(s: str) -> str:
    return {
        'starting': _fa('spinner') + s,
        'restarting': _fa('undo') + s,
        'running': _fa('toggle-on') + s,
        'job running': _fa('running') + s,
        'failure': _fa('bolt') + s,
        'paused': _fa('toggle-off') + s,
        'standby': _fa('power-off') + s,
    }.get(s, s)


def _format_job_state(s: str) -> str:
    return {
        'starting': _fa('spinner') + s,
        'running': _fa('running') + s,
        'success': _fa('check') + s,
        'failure': _fa('bolt') + s,
        'killed': _fa('skull-crossbones') + s,
    }.get(s, s)


def _format_datetime(d: datetime) -> str:
    td = (datetime.now().date() - d.date()).days
    word = {-1: 'Tomorrow', 0: 'Today', 1: 'Yesterday'}
    return (word[td] if td in word else '{:%-d %b %Y}'.format(d)) + ' at {:%H:%M}'.format(d)


def _format_date(d: date) -> str:
    td = (datetime.now().date() - d).days
    word = {-1: 'Tomorrow', 0: 'Today', 1: 'Yesterday'}
    return (word[td] if td in word else '{:%-d %b %Y}'.format(d))


def _format_duration(secs: float) -> str:
    d = relativedelta(seconds=int(secs), microseconds=int((secs % 1) * 1e6))
    attrs = {
        'years': 'years',
        'months': 'months',
        'days': 'days',
        'hours': 'hours',
        'minutes': 'mins',
        'seconds': 'secs',
        'microseconds': 'ms'
    }
    ret = ''
    for attr, attr_short in attrs.items():
        n = getattr(d, attr, 0)
        if n > 0:
            n = getattr(d, attr)
            if attr == 'microseconds':
                n /= 1000
            u = n != 1 and attr_short or attr_short[:-1]
            ret += f'{n:.0f} {u}, '
            if attr in {'minutes', 'seconds'}:
                break
    return ret.strip(', ')


def _format_size(x: Union[int, float]) -> str:
    if abs(x) == 1:
        return f'{x:.0f} byte'
    if abs(x) < 1000.0:
        return f'{x:.0f} bytes'
    x /= 1000.0
    for unit in ['KB', 'MB', 'GB', 'TB']:
        if abs(x) < 1000.0:
            return f'{x:.1f} {unit}'
        x /= 1000.0
    return f'{x:,.1f} PB'
