from __future__ import division
from collections import defaultdict
from datetime import timedelta, datetime
from itertools import groupby
import logging
import os
import codecs

from babelfish import Language
from dogpile.cache.backends.file import AbstractFileLock
from dogpile.util.readwrite_lock import ReadWriteMutex

from subliminal import (AsyncProviderPool, Episode, Movie, Video, check_video, get_scores,
                        refine, region, save_subtitles, scan_videos)
from subliminal.core import search_external_subtitles

from plexapi.server import PlexServer

from ndscheduler import job

class MutexLock(AbstractFileLock):
    """:class:`MutexLock` is a thread-based rw lock based on :class:`dogpile.core.ReadWriteMutex`."""
    def __init__(self, filename):
        self.mutex = ReadWriteMutex()

    def acquire_read_lock(self, wait):
        ret = self.mutex.acquire_read_lock(wait)
        return wait or ret

    def acquire_write_lock(self, wait):
        ret = self.mutex.acquire_write_lock(wait)
        return wait or ret

    def release_read_lock(self):
        return self.mutex.release_read_lock()

    def release_write_lock(self):
        return self.mutex.release_write_lock()

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
        return 'pid: %s | downloaded: %s' % (os.getpid(), str(result['subtitles']['total']))

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
                if len(languages - video.subtitle_languages) > 0:
                    videos.append(video)
                else:
                    ignored_videos.append(video)
            else:
                ignored_videos.append(video)
            
        if len(videos) > 0:
            result['videos']['collected'] = len(videos)
        if len(ignored_videos) > 0:
            result['videos']['ignored'] = len(ignored_videos)

        if videos:
            # download best subtitles
            downloaded_subtitles = defaultdict(list)
            with AsyncProviderPool(max_workers=max_workers, providers=providers, provider_configs=provider_configs) as p:
                for v in videos:
                    scores = get_scores(v)
                    subtitles = p.download_best_subtitles(p.list_subtitles(v, languages - v.subtitle_languages), v, languages, min_score=scores['hash'] * min_score / 100)
                    downloaded_subtitles[v] = subtitles

                if p.discarded_providers:
                    result['providers']['discarded'] = p.discarded_providers

            # save subtitles
            total_subtitles = 0
            for v, subtitles in downloaded_subtitles.items():
                saved_subtitles = save_subtitles(v, subtitles, directory=None, encoding=encoding)
                total_subtitles += len(saved_subtitles)

                for key, group in groupby(saved_subtitles, lambda x: x.provider_name):
                    result['subtitles'][os.path.split(v.name)[1]][key] = len(list(group))

                if plex and len(saved_subtitles) > 0:
                    #plex_video = plex.library.section('TV Shows').search(title=v.series, year=v.year, maxresults=1)[0].episode(season=v.season, episode=v.episode)
                    plex_video = plex.library.section('TV Shows').search(title=v.series, year=v.year, maxresults=1)[0].episode(title=v.title)
                    plex_video_text = '%s (%s) S%02dE%02d - %s' % (v.series, v.year, v.season, v.episode, v.title)
                    if plex_video:
                        plex_video.refresh()
                        if not result['plex']['refreshed']:
                            result['plex']['refreshed'] = []
                        result['plex']['refreshed'].append(plex_video_text)
                    else:                    
                        if not result['plex']['failed']:
                            result['plex']['failed'] = []
                        result['plex']['failed'].append(plex_video_text)
            
            result['subtitles']['total'] = total_subtitles

        scan_end = datetime.now()
        result['meta']['start'] = scan_start.isoformat()
        result['meta']['end'] = scan_end.isoformat()
        result['meta']['duration'] = str(scan_end - scan_start)
        return result