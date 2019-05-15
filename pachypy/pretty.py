__all__ = [
    'PrettyPachydermClient'
]

import logging
import re
from typing import Dict, List, Iterable, Union, Optional
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas.io.formats.style as style
import pandas as pd
import numpy as np
import yaml
from IPython.core.display import HTML
from termcolor import cprint
from tqdm import tqdm_notebook

from .client import PachydermClient, WildcardFilter


FONT_AWESOME_CSS_URL = 'https://use.fontawesome.com/releases/v5.8.1/css/all.css'
CLIPBOARD_JS_URL = 'https://cdnjs.cloudflare.com/ajax/libs/clipboard.js/2.0.4/clipboard.js'
BAR_COLOR = '#105ecd33'
PROGRESS_BAR_COLOR = '#03820333'

# Make yaml.dump() keep the order of keys in dictionaries
yaml.add_representer(
    dict,
    lambda self,
    data: yaml.representer.SafeRepresenter.represent_dict(self, data.items())  # type: ignore
)


def _fa(i: str) -> str:
    return f'<i class="fas fa-fw fa-{i}"></i>&nbsp;'


class CPrintHandler(logging.StreamHandler):

    def emit(self, record: logging.LogRecord):
        color = {
            logging.INFO: 'green',
            logging.WARNING: 'yellow',
            logging.ERROR: 'red',
            logging.CRITICAL: 'red',
        }.get(record.levelno, 'grey')
        cprint(self.format(record), color=color)


class PrettyTable(HTML):

    def __init__(self, styler: style.Styler, df: pd.DataFrame):
        super().__init__(data=styler.render())
        self.raw = df
        self.inject_dependencies()

    def inject_dependencies(self) -> None:
        fa_css = f'<link rel="stylesheet" href="{FONT_AWESOME_CSS_URL}" crossorigin="anonymous">'
        cb_js = f'''
            <script src="{CLIPBOARD_JS_URL}" crossorigin="anonymous"></script>
            <script>var clipboard = new ClipboardJS('.copyable');</script>
        '''
        self.data = fa_css + cb_js + self.data  # type: ignore


class PrettyYAML(HTML):

    def __init__(self, obj: object):
        super().__init__(data=self.format_yaml(obj))
        self.raw = obj

    @staticmethod
    def format_yaml(obj: object) -> str:
        s = str(yaml.dump(obj))
        s = re.sub(r'(^[\s-]*)([^\s]+:)', '\\1<span style="color: #888;">\\2</span>', s, flags=re.MULTILINE)
        return '<pre style="border: 1px #ccc solid; padding: 10px 12px; line-height: 140%;">' + s + '</pre>'


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

    def list_repos(self, repos: WildcardFilter = '*') -> PrettyTable:
        df = super().list_repos(repos=repos)
        dfr = df.copy()
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
            .format({'Created': self._format_datetime, 'Size': self._format_size}) \
            .set_properties(subset=['Branches'], **{'white-space': 'normal !important'}) \
            .set_table_styles(self.table_styles) \
            .hide_index()
        return PrettyTable(styler, dfr)

    def list_commits(self, repos: WildcardFilter, n: int = 10) -> PrettyTable:
        df = super().list_commits(repos=repos, n=n)
        dfr = df.copy()
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
                'Commit': self._format_hash,
                'Parent Commit': self._format_hash,
                'Branch': ', '.join,
                'Started': self._format_datetime,
                'Finished': self._format_datetime,
                'Size': self._format_size
            }) \
            .set_table_styles(self.table_styles) \
            .hide_index()
        return PrettyTable(styler, dfr)

    def list_files(self, repos: WildcardFilter, branch: Optional[str] = 'master', commit: Optional[str] = None,
                   glob: str = '**', files_only: bool = True) -> PrettyTable:
        df = super().list_files(repos=repos, branch=branch, commit=commit, glob=glob, files_only=files_only)
        dfr = df.copy()
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
                'Type': self._format_file_type,
                'Size': self._format_size,
                'Commit': self._format_hash,
                'Branch': ', '.join,
                'Committed': self._format_datetime
            }) \
            .set_properties(subset=['Path'], **{'white-space': 'normal !important'}) \
            .set_table_styles(self.table_styles) \
            .hide_index()
        return PrettyTable(styler, dfr)

    def list_pipelines(self, pipelines: WildcardFilter = '*') -> PrettyTable:
        df = super().list_pipelines(pipelines=pipelines)
        dfr = df.copy()
        df['sort_key'] = df.index.map(self._calc_pipeline_sort_key(df['input_repos'].to_dict()))
        df.sort_values('sort_key', inplace=True)
        df.rename({
            'pipeline': 'Pipeline',
            'state': 'State',
            'cron_spec': 'Cron',
            'cron_prev_tick': 'Last Tick',
            'cron_next_tick': 'Next Tick',
            'input': 'Input',
            'output_branch': 'Output',
            'datum_tries': 'Tries',
            'created': 'Created',
        }, axis=1, inplace=True)
        df.loc[df['jobs_running'] > 0, 'State'] = 'job running'
        now = datetime.now(self.user_timezone)
        df['Next Tick In'] = (now - df['Next Tick']).dt.total_seconds() * -1
        df['Parallelism'] = ''
        df.loc[df['parallelism_constant'] > 0, 'Parallelism'] = \
            _fa('hashtag') + df['parallelism_constant'].astype(str)
        df.loc[df['parallelism_coefficient'] > 0, 'Parallelism'] = \
            _fa('asterisk') + df['parallelism_coefficient'].astype(str)
        df['Jobs'] = \
            '<span style="color: green">' + df['jobs_success'].astype(str) + '</span>' + \
            np.where(df['jobs_failure'] > 0, ' + <span style="color: red">' + df['jobs_failure'].astype(str) + '</span>', '')
        styler = df[['Pipeline', 'State', 'Cron', 'Next Tick In', 'Input', 'Output', 'Parallelism', 'Jobs', 'Created']].style \
            .apply(self._style_pipeline_state, subset=['State']) \
            .format({
                'State': self._format_pipeline_state,
                'Cron': self._format_cron_spec,
                'Next Tick In': self._format_duration,
                'Created': self._format_datetime,
            }) \
            .set_properties(subset=['Input'], **{'white-space': 'normal !important'}) \
            .set_table_styles(self.table_styles) \
            .hide_index()
        return PrettyTable(styler, dfr)

    def list_jobs(self, pipelines: WildcardFilter = '*', n: int = 20, hide_null_jobs: bool = True) -> PrettyTable:
        df = super().list_jobs(pipelines=pipelines, n=n, hide_null_jobs=hide_null_jobs)
        dfr = df.copy()
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
            .apply(self._style_job_state, subset=['State']) \
            .apply(self._style_job_progress, subset=['Progress']) \
            .format({
                'Job': self._format_hash,
                'State': self._format_job_state,
                'Started': self._format_datetime,
                'Duration': self._format_duration,
                'Restarts': lambda i: _fa('undo') + str(i) if i > 0 else '',
                'Downloaded': self._format_size,
                'Uploaded': self._format_size,
                'Output Commit': self._format_hash
            }) \
            .set_table_styles(self.table_styles) \
            .hide_index()
        return PrettyTable(styler, dfr)

    def list_datums(self, job: str) -> PrettyTable:
        df = super().list_datums(job=job)
        dfr = df.copy()
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
            .apply(self._style_datum_state, subset=['State']) \
            .format({
                'Job': self._format_hash,
                'Datum': self._format_hash,
                'State': self._format_datum_state,
                'Type': self._format_file_type,
                'Size': self._format_size,
                'Commit': self._format_hash,
                'Committed': self._format_datetime
            }) \
            .set_properties(subset=['Path'], **{'white-space': 'normal !important'}) \
            .set_table_styles(self.table_styles) \
            .hide_index()
        return PrettyTable(styler, dfr)

    def get_logs(self, pipelines: WildcardFilter = '*', datum: Optional[str] = None,
                 last_job_only: bool = True, user_only: bool = False, master: bool = False, tail: int = 0) -> None:
        df = super().get_logs(pipelines=pipelines, last_job_only=last_job_only, user_only=user_only, master=master, tail=tail)
        job = None
        worker = None
        for _, row in df.iterrows():
            if row.job != job:
                print()
                cprint(f' Pipeline {row.pipeline} ' + (f'| Job {row.job} ' if row.job else ''), 'yellow', 'on_grey')
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

    def inspect_repo(self, repo: str) -> PrettyYAML:
        info = super().inspect_repo(repo)
        return PrettyYAML(info)

    def inspect_pipeline(self, pipeline: str) -> PrettyYAML:
        info = super().inspect_pipeline(pipeline)
        return PrettyYAML(info)

    def inspect_job(self, job: str) -> PrettyYAML:
        info = super().inspect_job(job)
        return PrettyYAML(info)

    def inspect_datum(self, job: str, datum: str) -> PrettyYAML:
        info = super().inspect_datum(job, datum)
        return PrettyYAML(info)

    @staticmethod
    def _calc_pipeline_sort_key(input_repos: Dict[str, List[str]]):
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

    def _format_datetime(self, d: datetime) -> str:
        if pd.isna(d):
            return ''
        td = (datetime.now(self.user_timezone).date() - d.date()).days
        word = {-1: 'Tomorrow', 0: 'Today', 1: 'Yesterday'}
        return (word[td] if td in word else f'{d:%-d %b %Y}') + f' at {d:%H:%M}'

    @staticmethod
    def _format_duration(secs: float, n: int = 2) -> str:
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
        i = 0
        for attr, attr_short in attrs.items():
            x = getattr(d, attr, 0)
            if x > 0:
                if attr == 'microseconds':
                    x /= 1000
                    u = attr_short
                else:
                    u = x != 1 and attr_short or attr_short[:-1]
                ret += f'{x:.0f} {u}, '
                i += 1
                if i >= n or attr in {'minutes', 'seconds'}:
                    break
        return ret.strip(', ')

    @staticmethod
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

    @staticmethod
    def _format_hash(s: str) -> str:
        if pd.isna(s):
            return ''
        short = s[:5] + '..' + s[-5:] if len(s) > 12 else s
        return f'<pre class="copyable" title="{s} (click to copy)" data-clipboard-text="{s}" style="cursor: copy; background: none; white-space: nowrap;">{short}</pre>'

    @staticmethod
    def _format_cron_spec(s: str) -> str:
        if pd.isna(s) or s == '':
            return ''
        return _fa('stopwatch') + s

    @staticmethod
    def _format_file_type(s: str) -> str:
        return {
            'file': _fa('file') + s,
            'dir': _fa('folder') + s,
        }.get(s, s)

    @staticmethod
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

    @staticmethod
    def _format_job_state(s: str) -> str:
        return {
            'unknown': _fa('question') + s,
            'starting': _fa('spinner') + s,
            'running': _fa('running') + s,
            'merging': _fa('compress-arrows-alt') + s,
            'success': _fa('check') + s,
            'failure': _fa('bolt') + s,
            'killed': _fa('skull-crossbones') + s,
        }.get(s, s)

    @staticmethod
    def _format_datum_state(s: str) -> str:
        return {
            'unknown': _fa('question') + s,
            'starting': _fa('spinner') + s,
            'skipped': _fa('forward') + s,
            'success': _fa('check') + s,
            'failed': _fa('bolt') + s,
        }.get(s, s)

    @staticmethod
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
        return [f"color: {color.get(v, 'gray')}; font-weight: bold" for v in s]

    @staticmethod
    def _style_job_state(s: Iterable[str]) -> List[str]:
        color = {
            'starting': 'orange',
            'running': 'orange',
            'merging': 'orange',
            'success': 'green',
            'failure': 'red',
            'killed': 'red',
        }
        return [f"color: {color.get(v, 'gray')}; font-weight: bold" for v in s]

    @staticmethod
    def _style_datum_state(s: Iterable[str]) -> List[str]:
        color = {
            'starting': 'orange',
            'skipped': '#0251c9',
            'success': 'green',
            'failed': 'red',
        }
        return [f"color: {color.get(v, 'gray')}; font-weight: bold" for v in s]

    @staticmethod
    def _style_job_progress(s: pd.Series) -> List[str]:
        def css_bar(end):
            css = 'width: 10em; height: 80%;'
            if end > 0:
                css += 'background: linear-gradient(90deg,'
                css += '{c} {e:.1f}%, transparent {e:.1f}%)'.format(e=min(end, 100), c=PROGRESS_BAR_COLOR)
            return css
        s = s.apply(lambda x: float(x.split('%')[0]))
        return [css_bar(x) if not pd.isna(x) and x < 100 else '' for x in s]

    @classmethod
    def _progress(cls, x, n: Optional[int] = None, **kwargs):
        if n is None:
            try:
                n = len(x)
            except TypeError:
                return x
        if 'leave' not in kwargs:
            kwargs['leave'] = False
        return tqdm_notebook(x, total=n, **kwargs) if n > 2 else x
