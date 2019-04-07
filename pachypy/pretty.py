__all__ = [
    'PrettyPachydermClient'
]

import logging
from typing import Dict, List, Iterable, Union, Optional
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

import pandas.io.formats.style as style
import pandas as pd
import numpy as np
from IPython.core.display import HTML
from termcolor import cprint
from tqdm import tqdm_notebook

from .client import PachydermClient, WildcardFilter


FONT_AWESOME_CSS_URL = 'https://use.fontawesome.com/releases/v5.8.1/css/all.css'
CLIPBOARD_JS_URL = 'https://cdnjs.cloudflare.com/ajax/libs/clipboard.js/2.0.4/clipboard.js'
BAR_COLOR = '#105ecd33'
PROGRESS_BAR_COLOR = '#03820333'


class CPrintHandler(logging.StreamHandler):

    def emit(self, record: logging.LogRecord):
        color = {
            logging.INFO: 'green',
            logging.WARNING: 'yellow',
            logging.ERROR: 'red',
            logging.CRITICAL: 'red',
        }.get(record.levelno, 'grey')
        cprint(self.format(record), color=color)


class PrettyOutput(HTML):

    def __init__(self, styler: style.Styler, df: pd.DataFrame):
        super().__init__(data=styler.render())
        self.df = df
        self.inject_dependencies()

    def inject_dependencies(self) -> None:
        fa_css = f'<link rel="stylesheet" href="{FONT_AWESOME_CSS_URL}" crossorigin="anonymous">'
        cb_js = f'''
            <script src="{CLIPBOARD_JS_URL}" crossorigin="anonymous"></script>
            <script>var clipboard = new ClipboardJS('.copyable');</script>
        '''
        self.data = fa_css + cb_js + self.data  # type: ignore


class PrettyPachydermClient(PachydermClient):

    table_styles = [
        dict(selector='th', props=[('text-align', 'left'), ('white-space', 'nowrap')]),
        dict(selector='td', props=[('text-align', 'left'), ('white-space', 'nowrap'), ('padding-right', '20px')]),
    ]

    @property
    def logger(self):
        if self._logger is None:
            self._logger = logging.getLogger('pachypy')
            self._logger.handlers = [CPrintHandler()]
            self._logger.setLevel(logging.DEBUG)
            self._logger.propagate = False
        return self._logger

    def list_repos(self, repos: WildcardFilter = '*') -> PrettyOutput:
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
        styler = df[['Repo', 'Tick', 'Branches', 'Size', 'Created']].style \
            .bar(subset=['Size'], color=BAR_COLOR, vmin=0) \
            .format({'Created': _format_datetime, 'Size': _format_size}) \
            .set_properties(subset=['Branches'], **{'white-space': 'normal !important'}) \
            .set_table_styles(self.table_styles) \
            .hide_index()
        return PrettyOutput(styler, df)

    def list_commits(self, repos: WildcardFilter, n: int = 10) -> PrettyOutput:
        df = super().list_commits(repos=repos, n=n).reset_index()
        df.rename({
            'repo': 'Repo',
            'commit': 'Commit',
            'branches': 'Branch',
            'size_bytes': 'Size',
            'started': 'Started',
            'finished': 'Finished',
            'parent_commit': 'Parent Commit',
        }, axis=1, inplace=True)
        styler = df[['Repo', 'Commit', 'Branch', 'Size', 'Started', 'Finished', 'Parent Commit']].style \
            .bar(subset=['Size'], color=BAR_COLOR, vmin=0) \
            .format({
                'Commit': _hash,
                'Parent Commit': _hash,
                'Branch': ', '.join,
                'Started': _format_datetime,
                'Finished': _format_datetime,
                'Size': _format_size
            }) \
            .set_table_styles(self.table_styles) \
            .hide_index()
        return PrettyOutput(styler, df)

    def list_files(self, repos: WildcardFilter, branch: Optional[str] = 'master', commit: Optional[str] = None,
                   glob: str = '**', files_only: bool = True) -> PrettyOutput:
        df = super().list_files(repos=repos, branch=branch, commit=commit, glob=glob, files_only=files_only).reset_index()
        df.rename({
            'repo': 'Repo',
            'type': 'Type',
            'path': 'Path',
            'size_bytes': 'Size',
            'commit': 'Commit',
            'branches': 'Branch',
            'committed': 'Committed',
        }, axis=1, inplace=True)
        styler = df[['Repo', 'Commit', 'Branch', 'Type', 'Path', 'Size', 'Committed']].style \
            .bar(subset=['Size'], color=BAR_COLOR, vmin=0) \
            .format({
                'Type': _format_file_type,
                'Size': _format_size,
                'Commit': _hash,
                'Branch': ', '.join,
                'Committed': _format_datetime
            }) \
            .set_properties(subset=['Path'], **{'white-space': 'normal !important'}) \
            .set_table_styles(self.table_styles) \
            .hide_index()
        return PrettyOutput(styler, df)

    def list_pipelines(self, pipelines: WildcardFilter = '*') -> PrettyOutput:
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
        styler = df[['Pipeline', 'State', 'Cron', 'Input', 'Output', 'Tries', 'Parallelism', 'Jobs', 'Created']].style \
            .apply(_style_pipeline_state, subset=['State']) \
            .format({'State': _format_pipeline_state, 'Created': _format_datetime}) \
            .set_properties(subset=['Input'], **{'white-space': 'normal !important'}) \
            .set_table_styles(self.table_styles) \
            .hide_index()
        return PrettyOutput(styler, df)

    def list_jobs(self, pipelines: WildcardFilter = '*', n: int = 20) -> PrettyOutput:
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
            'output_commit': 'Output Commit',
        }, axis=1, inplace=True)
        df['Duration'] = df['Duration'].dt.total_seconds()
        df['Progress'] = \
            df['progress'].fillna(0).apply(lambda x: f'{x:.0%}') + ' | ' + \
            '<span style="color: green">' + df['data_processed'].astype(str) + '</span>' + \
            np.where(df['data_skipped'] > 0, ' + <span style="color: purple">' + df['data_skipped'].astype(str) + '</span>', '') + \
            ' / <span>' + df['data_total'].astype(str) + '</span>'
        styler = df[['Job', 'Pipeline', 'State', 'Started', 'Duration', 'Progress', 'Restarts', 'Downloaded', 'Uploaded', 'Output Commit']].style \
            .bar(subset=['Duration'], color=BAR_COLOR, vmin=0) \
            .apply(_style_job_state, subset=['State']) \
            .apply(_style_job_progress, subset=['Progress']) \
            .format({
                'Job': _hash,
                'State': _format_job_state,
                'Started': _format_datetime,
                'Duration': _format_duration,
                'Restarts': lambda i: _fa('undo') + str(i) if i > 0 else '',
                'Downloaded': _format_size,
                'Uploaded': _format_size,
                'Output Commit': _hash
            }) \
            .set_table_styles(self.table_styles) \
            .hide_index()
        return PrettyOutput(styler, df)

    def list_datums(self, job: str) -> PrettyOutput:
        df = super().list_datums(job=job).reset_index()
        df.rename({
            'job': 'Job',
            'datum': 'Datum',
            'state': 'State',
            'repo': 'Repo',
            'type': 'Type',
            'path': 'Path',
            'size_bytes': 'Size',
            'commit': 'Commit',
            'committed': 'Committed',
        }, axis=1, inplace=True)
        styler = df[['Job', 'Datum', 'State', 'Repo', 'Type', 'Path', 'Size', 'Commit', 'Committed']].style \
            .bar(subset=['Size'], color=BAR_COLOR, vmin=0) \
            .apply(_style_datum_state, subset=['State']) \
            .format({
                'Job': _hash,
                'Datum': _hash,
                'State': _format_datum_state,
                'Type': _format_file_type,
                'Size': _format_size,
                'Commit': _hash,
                'Committed': _format_datetime
            }) \
            .set_properties(subset=['Path'], **{'white-space': 'normal !important'}) \
            .set_table_styles(self.table_styles) \
            .hide_index()
        return PrettyOutput(styler, df)

    def get_logs(self, pipelines: WildcardFilter = '*', datum: Optional[str] = None,
                 last_job_only: bool = True, user_only: bool = False, master: bool = False) -> None:
        df = super().get_logs(pipelines=pipelines, last_job_only=last_job_only, user_only=user_only, master=master)
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
            if 'warning' in message.lower():
                color = 'magenta'
            elif 'error' in message.lower() or 'exception' in message.lower() or 'critical' in message.lower():
                color = 'red'
            cprint(f'[{row.ts}] {message}', color)
            job = row.job
            worker = row.worker

    @classmethod
    def _progress(cls, x, **kwargs):
        if 'leave' not in kwargs:
            kwargs['leave'] = False
        return tqdm_notebook(x, **kwargs) if len(x) > 2 else x


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


def _hash(s: str) -> str:
    if pd.isna(s):
        return ''
    short = s[:5] + '..' + s[-5:] if len(s) > 12 else s
    return f'<pre class="copyable" title="{s} (click to copy)" data-clipboard-text="{s}" style="cursor: copy; background: none; white-space: nowrap;">{short}</pre>'


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


def _style_datum_state(s: Iterable[str]) -> List[str]:
    color = {
        'starting': 'orange',
        'skipped': '#0251c9',
        'success': 'green',
        'failed': 'red',
    }
    return ['color: {c}; font-weight: bold'.format(c=color.get(v, 'gray')) for v in s]


def _style_job_progress(s: pd.Series) -> List[str]:
    def css_bar(end):
        css = 'width: 10em; height: 80%;'
        if end > 0:
            css += 'background: linear-gradient(90deg,'
            css += '{c} {e:.1f}%, transparent {e:.1f}%)'.format(e=min(end, 100), c=PROGRESS_BAR_COLOR)
        return css
    s = s.apply(lambda x: float(x.split('%')[0]))
    return [css_bar(x) if not pd.isna(x) and x < 100 else '' for x in s]


def _format_file_type(s: str) -> str:
    return {
        'file': _fa('file') + s,
        'dir': _fa('folder') + s,
    }.get(s, s)


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
        'unknown': _fa('question') + s,
        'starting': _fa('spinner') + s,
        'running': _fa('running') + s,
        'success': _fa('check') + s,
        'failure': _fa('bolt') + s,
        'killed': _fa('skull-crossbones') + s,
    }.get(s, s)


def _format_datum_state(s: str) -> str:
    return {
        'unknown': _fa('question') + s,
        'starting': _fa('spinner') + s,
        'skipped': _fa('forward') + s,
        'success': _fa('check') + s,
        'failed': _fa('bolt') + s,
    }.get(s, s)


def _format_datetime(d: datetime) -> str:
    if pd.isna(d):
        return ''
    td = (datetime.now().date() - d.date()).days
    word = {-1: 'Tomorrow', 0: 'Today', 1: 'Yesterday'}
    return (word[td] if td in word else '{:%-d %b %Y}'.format(d)) + ' at {:%H:%M}'.format(d)


def _format_date(d: date) -> str:
    if pd.isna(d):
        return ''
    td = (datetime.now().date() - d).days
    word = {-1: 'Tomorrow', 0: 'Today', 1: 'Yesterday'}
    return word[td] if td in word else '{:%-d %b %Y}'.format(d)


def _format_duration(secs: float) -> str:
    if pd.isna(secs):
        return ''
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
                u = attr_short
            else:
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
