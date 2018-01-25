from __future__ import division
from collections import defaultdict
from datetime import timedelta, datetime
from itertools import groupby
import os
import codecs
import sys
import hashlib

from babelfish import Language
from tinydb import TinyDB, Query

from subliminal import (AsyncProviderPool, Episode, Movie, Video, check_video, get_scores,
                        refine, region, save_subtitles, scan_videos)
from subliminal.core import search_external_subtitles
from subliminal.subtitle import get_subtitle_path

from plexapi.server import PlexServer
from plexapi.exceptions import BadRequest, NotFound
from plexapi.library import MovieSection, ShowSection

from ndscheduler import job

import aeidon

from .mutexlock import MutexLock

class ScanJob(job.JobBase):
    @classmethod
    def get_scheduled_description(cls):
        return 'pid: %s' % (os.getpid(), )

    @classmethod
    def get_scheduled_error_description(cls):
        return 'pid: %s | exception: %s' % (os.getpid(), sys.exc_info()[0].__name__)

    @classmethod
    def get_running_description(cls):
        return 'pid: %s' % (os.getpid(), )

    @classmethod
    def get_failed_description(cls):
        return 'pid: %s | exception: %s' % (os.getpid(), sys.exc_info()[0].__name__)

    @classmethod
    def get_succeeded_description(cls, result=None):
        total = result['subtitles']['total'] if result['subtitles']['total'] else 0
        return 'pid: %s | downloaded: %s' % (os.getpid(), str(total))

    @classmethod
    def meta_info(cls):
        return {
            'job_class_string': '%s.%s' % (cls.__module__, cls.__name__),
            'notes': 'Subliminal scanner with optional plex support',
            'arguments': [
                {'type': 'string', 'description': 'Path to scan for new subtitles'},            # scan_path
                {'type': 'int', 'description': 'Maximum file age in weeks'},                    # scan_age
                {'type': 'string', 'description': 'List of alpha3 language codes'},             # languages
                {'type': 'string', 'description': 'Encoding for saving subtitles'},             # encoding
                {'type': 'int', 'description': 'Minimum subtitle score'},                       # min_score
                {'type': 'string', 'description': 'List of providers to use'},                  # providers
                {'type': 'string', 'description': 'Dictionary with usernames and passwords'},   # prodiver_configs
                {'type': 'int', 'description': 'Number of scanner workers'},                    # max_workers
                {'type': 'string', 'description': 'Plex Server URL (optional)'},                # plex_url
                {'type': 'string', 'description': 'Plex token (optional)'}                      # plex_token
            ],
            'example_arguments': ''
        }

    def run(self, scan_path, scan_age, languages, encoding, min_score, providers, provider_configs, max_workers, plex_url=None, plex_token=None, *args, **kwargs):
        if not os.path.isdir(scan_path):
            raise IOError('Path \'%s\' doesn\'t exist!' % scan_path)
        if not scan_age >= 1:
            raise ValueError('\'scan_age\' must by at least 1!')
        if not len(languages) >= 1:
            raise ValueError('\'languages\' list can\'t be empty!')
        if not providers:
            raise ValueError('\'providers\' argument can\'t be empty!')
        if not max_workers >= 1:
            raise ValueError('\'max_workers\' must be at least 1!')

        if not provider_configs:
            provider_configs = {}

        __tree_dict = lambda: defaultdict(__tree_dict)
        result = __tree_dict()

        encoding = codecs.lookup(encoding).name
        age = timedelta(weeks=scan_age)
        languages = set([Language(l) for l in languages])

        plex = None
        if plex_url and plex_token:
            plex = PlexServer(plex_url, plex_token)

        scan_start = datetime.now()

        videos = []
        ignored_videos = []

        if not region.is_configured:
            region.configure('dogpile.cache.dbm', expiration_time=timedelta(days=30), arguments={'filename': 'subliminal.dbm', 'lock_factory': MutexLock})

        # scan videos
        scanned_videos = scan_videos(scan_path, age=age)

        for video in scanned_videos:
            video.subtitle_languages |= set(search_external_subtitles(video.name).values())
            if check_video(video, languages=languages, age=age, undefined=False):
                refine(video)
                if languages - video.subtitle_languages:
                    videos.append(video)
                else:
                    ignored_videos.append(video)
            else:
                ignored_videos.append(video)

        if videos:
            result['videos']['collected'] = [os.path.split(v.name)[1] for v in videos]
        if ignored_videos:
            result['videos']['ignored'] = len(ignored_videos)

        if videos:
            # download best subtitles
            downloaded_subtitles = defaultdict(list)
            with AsyncProviderPool(max_workers=max_workers, providers=providers, provider_configs=provider_configs) as p:
                for video in videos:
                    scores = get_scores(video)
                    subtitles_to_download = p.list_subtitles(video, languages - video.subtitle_languages)
                    downloaded_subtitles[video] = p.download_best_subtitles(subtitles_to_download, video, languages, min_score=scores['hash'] * min_score / 100)

                if p.discarded_providers:
                    result['providers']['discarded'] = list(p.discarded_providers)

            # filter subtitles
            with TinyDB('subtitle_db.json') as db:
                table = db.table('downloaded')
                query = Query()
                for video, subtitles in downloaded_subtitles.items():
                    discarded_subtitles = list()
                    discarded_subtitles_info = list()

                    for s in subtitles:
                        subtitle_hash = hashlib.sha256(s.content).hexdigest()
                        subtitle_file = get_subtitle_path(os.path.split(video.name)[1], s.language)
                        dbo = {'hash': subtitle_hash, 'file': subtitle_file}
                        if table.search((query.hash == subtitle_hash) & (query.file == subtitle_file)):
                            discarded_subtitles.append(s)
                            discarded_subtitles_info.append(dbo)
                        else:
                            table.insert(dbo)

                    downloaded_subtitles[video] = [x for x in subtitles if x not in discarded_subtitles]
                    if discarded_subtitles_info:
                        result['subtitles']['discarded'] = result['subtitles'].get('discarded', []) + discarded_subtitles_info

            downloaded_subtitles = {k: v for k,v in downloaded_subtitles.items() if v}

            # save subtitles
            saved_subtitles = {}
            for video, subtitles in downloaded_subtitles.items():
                saved_subtitles[video] = save_subtitles(video, subtitles, directory=None, encoding=encoding)

                for key, group in groupby(saved_subtitles[video], lambda x: x.provider_name):
                    subtitle_filenames = [get_subtitle_path(os.path.split(video.name)[1], s.language) for s in list(group)]
                    result['subtitles'][key] = result['subtitles'].get(key, []) + subtitle_filenames
            result['subtitles']['total'] = sum(len(v) for v in saved_subtitles.values())

            # refresh plex
            for video, subtitles in saved_subtitles.items():
                if plex and subtitles:
                    item_found = False
                    for section in plex.library.sections():
                        try:
                            if isinstance(section, MovieSection) and isinstance(video, Movie):
                                plex_item = section.search(title=video.title, year=video.year, libtype='movie', sort='addedAt:desc', maxresults=1)[0]
                            elif isinstance(section, ShowSection) and isinstance(video, Episode):
                                plex_show = section.search(title=video.series, year=video.year, libtype='show', sort='addedAt:desc', maxresults=1)[0]
                                plex_episodes = [e for e in plex_show.episodes() if int(e.seasonNumber) == video.season and int(e.index) == video.episode]
                                if len(plex_episodes) != 1:
                                    raise NotFound
                                plex_item = plex_episodes[0]
                            else:
                                continue
                        except NotFound:
                            continue
                        except BadRequest:
                            continue

                        if plex_item:
                            plex_item.refresh()
                            result['plex']['refreshed'] = result['plex'].get('refreshed', []) + ['%s%s' % (repr(plex_item.section()), repr(video))]
                            item_found = True

                    if not item_found:
                        result['plex']['failed'] = result['plex'].get('failed', []) + [repr(video)]

            # convert subtitles
            for video, subtitles in saved_subtitles.items():
                target_format = aeidon.formats.SUBRIP
                for s in subtitles:
                    subtitle_path = get_subtitle_path(video.name, s.language)
                    source_format = aeidon.util.detect_format(subtitle_path, encoding)
                    source_file = aeidon.files.new(source_format, subtitle_path, aeidon.encodings.detect_bom(subtitle_path) or encoding)

                    if source_format != target_format:
                        format_info = {'file': get_subtitle_path(os.path.split(video.name)[1], s.language), 'from': source_format.label, 'to': target_format.label}
                        result['subtitles']['converted'] = result['subtitles'].get('converted', []) + [format_info]

                    aeidon_subtitles = source_file.read()
                    for f in [aeidon.formats.SUBRIP, aeidon.formats.MICRODVD, aeidon.formats.MPL2]:
                        markup = aeidon.markups.new(f)
                        for s in aeidon_subtitles:
                            s.main_text = markup.decode(s.main_text)

                    markup = aeidon.markups.new(target_format)
                    for s in aeidon_subtitles:
                        s.main_text = markup.encode(s.main_text)

                    target_file = aeidon.files.new(target_format, subtitle_path, encoding)
                    target_file.write(aeidon_subtitles, aeidon.documents.MAIN)

        scan_end = datetime.now()
        result['meta']['start'] = scan_start.isoformat()
        result['meta']['end'] = scan_end.isoformat()
        result['meta']['duration'] = str(scan_end - scan_start)
        return result
