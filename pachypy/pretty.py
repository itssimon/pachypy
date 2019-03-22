__all__ = [
    'PrettyPachydermClient'
]

from datetime import datetime

import pandas.io.formats.style as style
from emoji import emojize
from termcolor import cprint

from .client import PachydermClient


STYLE_HIGHLIGHT_CSS = 'color: #d65f5f; font-weight: bold'
STYLE_BAR_COLOR = '#d65f5f44'


class PrettyPachydermClient(PachydermClient):

    def list_repos(self, repos: str = '*') -> style.Styler:
        df = super().list_repos(repos=repos)
        return df.style \
            .apply(_style_highlight_gt1, subset=['branches']) \
            .apply(_style_highlight_true, subset=['is_tick']) \
            .bar(subset=['size_megabytes'], color=STYLE_BAR_COLOR)

    def list_pipelines(self, pipelines: str = '*') -> style.Styler:
        df = super().list_pipelines(pipelines=pipelines).reset_index()
        df['Parallelism'] = ''
        df.loc[df['parallelism_constant'] > 0, 'Parallelism'] = \
            emojize(':keycap_#: ', use_aliases=True) + df['parallelism_constant'].astype(str)
        df.loc[df['parallelism_coefficient'] > 0, 'Parallelism'] = \
            emojize(':keycap_*: ', use_aliases=True) + df['parallelism_coefficient'].astype(str)
        df['Jobs S/F'] = \
            '<span style="' + df['jobs_success'].apply(lambda x: 'color: green' if x > 0 else '') + '">' + \
            df['jobs_success'].astype(str) + \
            '</span> / <span style="' + df['jobs_failure'].apply(lambda x: 'color: red' if x > 0 else '') + '">' + \
            df['jobs_failure'].astype(str) + \
            '</span>'
        df.rename({
            'pipeline': 'Pipeline',
            'state': 'State',
            'cron_spec': 'Cron',
            'input': 'Input',
            'output_branch': 'Output',
            'datum_tries': 'Tries',
            'created': 'Created',
        }, axis=1, inplace=True)
        df[['State']] = df[['State']].replace({
            'starting': emojize(':hourglass: starting', use_aliases=True),
            'running': emojize(':heavy_check_mark: running', use_aliases=True),
            'restarting': emojize(':hourglass: restarting', use_aliases=True),
            'failure': emojize(':bangbang: failure', use_aliases=True),
            'paused': emojize(':hand: paused', use_aliases=True),
            'standby': emojize(':zzz: standby'),
        })
        df.loc[2, 'jobs_running'] = 1
        df.loc[df['jobs_running'] > 0, 'State'] = emojize(':running: job running', use_aliases=True),
        df.loc[df['Cron'] != '', 'Cron'] = emojize(':stopwatch:') + ' ' + df['Cron']
        return df[['Pipeline', 'State', 'Cron', 'Input', 'Output', 'Tries', 'Parallelism', 'Jobs S/F', 'Created']].style \
            .format({'Created': _format_date_natural}) \
            .apply(_style_state, subset=['State']) \
            .set_properties(**{'text-align': 'left'}) \
            .set_properties(subset=['State', 'Cron', 'Jobs S/F'], **{'white-space': 'nowrap'}) \
            .set_table_styles([dict(selector='th', props=[('text-align', 'left')])]) \
            .hide_index()

    def list_jobs(self, pipelines: str = '*', n: int = 20) -> style.Styler:
        df = super().list_jobs(pipelines=pipelines, n=n)
        return df.style \
            .apply(_style_state, subset=['state']) \
            .apply(_style_highlight_nonzero, subset=['restart']) \
            .apply(_style_highlight_zero, subset=['data_processed']) \
            .bar(subset=['download_time', 'process_time', 'upload_time'], color=STYLE_BAR_COLOR)

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


def _format_date_natural(d):
    td = (datetime.now().date() - d.date()).days
    word = {-1: 'Tomorrow', 0: 'Today', 1: 'Yesterday'}
    return (word[td] if td in word else '{:%-d %b %Y}'.format(d)) + ' at {:%H:%M}'.format(d)


def _style_highlight_true(s):
    return [STYLE_HIGHLIGHT_CSS if v else '' for v in s]


def _style_highlight_gt1(s):
    return [STYLE_HIGHLIGHT_CSS if v else '' for v in s > 1]


def _style_highlight_nonzero(s):
    return [STYLE_HIGHLIGHT_CSS if v else '' for v in s != 0]


def _style_highlight_zero(s):
    return [STYLE_HIGHLIGHT_CSS if v else '' for v in s == 0]


def _style_state(s):
    color = {
        'starting': 'orange',
        'running': 'green',
        'success': 'green',
        'restarting': 'orange',
        'failure': 'red',
        'paused': 'orange',
        'standby': '#0251c9',
        'killed': 'magenta',
    }
    state = lambda x: x.split(' ')[-1]
    return [f'color: {color[state(v)]}; font-weight: bold' if state(v) in color else '' for v in s]
