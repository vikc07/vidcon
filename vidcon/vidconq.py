"""
Video Converter queue manager
todo:
handling of vob and dat files
"""

from sqlalchemy import *
from datetime import datetime
import json
import os
import shutil
from gpm import formatting
from lib import func


def do():
    err = 0

    # get all files in the queue: complete or incomplete
    queue = func.get_all_files_in_queue()

    # Query downloads folder
    movies = []
    movies_not_converted = []
    movies_success = []
    log.info('fetching all files')
    for filename in func.get_files():
        ext = func.get_file_extension(filename)
        fsize = formatting.fsize_pretty(os.stat(filename).st_size, return_size_only=True, unit='gb')
        # Is it a file, of required file type and not already in the queue? If yes, then proceed
        if os.path.isfile(filename) and (ext in cfg.VIDCON_FILE_TYPES) and (filename not in queue):
            movie = {}
            if ((ext != '.m2ts') and (fsize > 0.5)) or (ext == '.m2ts') or (fsize > 2.5):
                log.info(ext.strip('.') + ': ' + filename)
                movie['path'] = filename
                movie['title'] = func.get_file_name_without_extension(filename)
                movies.append(movie)
            else:
                log.debug('skipped: ' + filename)
        else:
            log.debug('skipped: ' + filename)

    log.info('processing')
    for movie in movies:
        convert_audio = False
        convert_video = False
        movie_name = movie['path']
        movie_title = movie['title']
        ext = func.get_file_extension(movie_name)
        log.info('movie: ' + movie_name)
        log.info('title: ' + movie_title)

        try:
            ffprobe_success, ffprobe = func.ffprobe(movie_name)
            log.debug('ffprobe output ' + ffprobe)
        except Exception as e:
            log.error('error occcurred running ffprobe')
            log.error(e)
            err = 1
        else:
            if ffprobe_success:
                movie_info = json.loads(ffprobe)
                log.info('container: ' + movie_info['format']['format_name'])
                for stream in movie_info['streams']:
                    stream_type = stream['codec_type']
                    if stream_type in ['audio', 'video']:
                        codec = stream['codec_name']
                        if codec != 'mjpeg':
                            not_supported = ''
                            if (stream_type == 'video') and (codec not in cfg.VIDCON_OK_V_FORMATS):
                                not_supported = '**not supported'
                                convert_video = True
                            elif (stream_type == 'audio') and (codec not in cfg.VIDCON_OK_A_FORMATS):
                                not_supported = '**not supported'
                                convert_audio = True
                            log.info('stream: ' + str(stream['index']))
                            log.info('type: ' + stream['codec_type'])
                            log.info('codec: ' + codec + ' ' + not_supported)

                vcodec = 'copy'
                acodec = 'copy'
                complete_flag = False
                ts_complete = None
                no_conversion_required = False
                update_queue = True

                # output file will be of format title/title.ext
                output_folder = func.get_file_path(movie_name)
                output_file = movie_name
                log.debug('output folder: {}'.format(output_folder))

                if convert_audio or convert_video:
                    log.info('conversion required')
                    if convert_video:
                        vcodec = cfg.VIDCON_DEFALUT_VCODEC

                    if convert_audio:
                        acodec = cfg.VIDCON_DEFALUT_ACODEC

                    output_file = os.path.join(output_folder, movie_title + cfg.VIDCON_DEFAULT_EXT)
                    log.info('output file will be: ' + output_file)
                elif ext in ['.vob', '.m2ts']:
                    # Still convert to mkv to avoid transcoding during playback through Plex
                    output_file = os.path.join(output_folder, movie_title + cfg.VIDCON_DEFAULT_EXT)
                    log.info('output file will be: ' + output_file)
                else:
                    log.info('no conversion required')
                    no_conversion_required = True
                    complete_flag = True
                    ts_complete = datetime.now()
                    movies_not_converted.append(movie_title)

                # Update queue
                row = {
                    'input_file': movie_name,
                    'output_file': output_file,
                    'title': movie_title,
                    'vcodec': vcodec,
                    'acodec': acodec,
                    'complete_flag': complete_flag,
                    'ts_complete': ts_complete
                }
                queue_pos = func.add_to_queue(row)
                if (len(queue_pos) > 0) and (not no_conversion_required):
                    log.info('queue position: ' + str(queue_pos[0]))
                    movies_success.append(movie_title)

            else:
                log.error('error occurred while running ffprobe')
                err = 1

    if len(movies_success) > 0 or len(movies_not_converted) > 0:
        subject = 'Video Converter Queue Manager update'
        msg = ''

        if len(movies_success) > 0:
            msg = msg + 'Following movies were queued for conversion:\n'
            for movie in movies_success:
                msg = msg + movie + '\n'
            msg = msg + '\n'

        if len(movies_not_converted) > 0:
            msg = msg + 'No conversion required for following movies:\n'
            for movie in movies_not_converted:
                msg = msg + movie + '\n'
            msg = msg + '\n'

        log.info('sending alert')
        # Send email
        try:
            func.sendalert(subject, msg)
        except Exception as e:
            log.error('error occurred ending alert')
            log.error(e)
            err = 1
        else:
            log.info('alert sent')

    func.close_queue()
    return err


if __name__ == '__main__':
    func.quit_if_already_running(__file__)
    func.create_lock_file(__file__)
    cfg = func.read_cfg(__file__)
    log = func.init_log(__file__)

    log.start()
    err = do()
    log.end()
    func.remove_lock_file(__file__)

    exit(err)

