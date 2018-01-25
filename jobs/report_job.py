from __future__ import division
from collections import defaultdict
from datetime import timedelta, datetime
from itertools import groupby
import os
import codecs
import sys
import hashlib

from babelfish import Language

from subliminal import (AsyncProviderPool, Episode, Movie, Video, check_video, get_scores,
                        refine, region, save_subtitles, scan_videos)
from subliminal.core import search_external_subtitles
from subliminal.subtitle import get_subtitle_path

from ndscheduler import job

from .mutexlock import MutexLock

class ReportJob(job.JobBase):
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
        return 'pid: %s' % (os.getpid(), )

    @classmethod
    def meta_info(cls):
        return {
            'job_class_string': '%s.%s' % (cls.__module__, cls.__name__),
            'notes': 'Subliminal report',
            'arguments': [
                {'type': 'string', 'description': 'Path to scan for new subtitles'},            # scan_path
                {'type': 'int', 'description': 'Maximum file age in weeks'},                    # scan_age
                {'type': 'string', 'description': 'List of alpha3 language codes'},             # languages
            ],
            'example_arguments': ''
        }

    def run(self, scan_path, scan_age, languages, *args, **kwargs):
        if not os.path.isdir(scan_path):
            raise IOError('Path \'%s\' doesn\'t exist!' % scan_path)
        if not scan_age >= 1:
            raise ValueError('\'scan_age\' must by at least 1!')
        if not len(languages) >= 1:
            raise ValueError('\'languages\' list can\'t be empty!')

        __tree_dict = lambda: defaultdict(__tree_dict)
        result = __tree_dict()

        age = timedelta(weeks=scan_age)
        languages = set([Language(l) for l in languages])

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
            result['videos']['ignored'] = [os.path.split(v.name)[1] for v in ignored_videos]

        scan_end = datetime.now()
        result['meta']['start'] = scan_start.isoformat()
        result['meta']['end'] = scan_end.isoformat()
        result['meta']['duration'] = str(scan_end - scan_start)
        return result
