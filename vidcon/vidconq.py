"""
Video Converter queue manager
todo:
handling of vob and dat files
"""

from sqlalchemy import *
from datetime import datetime
import json
import os
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
        fsize_in_bytes = os.stat(filename).st_size
        fsize = formatting.fsize_pretty(fsize_in_bytes, return_size_only=True, unit='gb')
        # Is it a file, of required file type and not already in the queue? If yes, then proceed
        if os.path.isfile(filename) and (ext in cfg.VIDCON_FILE_TYPES) and (filename not in queue):
            movie = {}
            if ((ext != '.m2ts') and (fsize > 0.5)) or (ext == '.m2ts') or (fsize > 2.5):
                log.info(ext.strip('.') + ': ' + filename)
                movie['path'] = filename
                movie['title'] = func.get_file_name_without_extension(filename)
                movie['orig_fsize'] = fsize_in_bytes
                movies.append(movie)
            else:
                log.debug('skipped: ' + filename)
        else:
            log.debug('skipped: ' + filename)

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
                orig_num_of_streams = len(movie_info['streams'])
                orig_num_of_astreams = 0
                orig_num_of_vstreams = 0
                orig_num_of_sstreams = 0
                orig_num_of_ostreams = 0
                orig_vcodec_name = ''
                orig_vcodec_profile = ''
                orig_vcodec_width = 0
                orig_vcodec_height = 0
                orig_vcodec_aspect_ratio = ''
                orig_vcodec_pix_fmt = ''
                orig_vcodec_level = ''
                orig_acodec_name = ''
                orig_acodec_sample_fmt = ''
                orig_acodec_sample_rate = ''
                orig_acodec_channels = 0
                orig_acodec_channel_layout = ''
                orig_acodec_bit_rate = ''
                for stream in movie_info['streams']:
                    stream_type = stream['codec_type']
                    if stream_type in ['audio', 'video']:
                        codec = stream['codec_name']

                        if stream_type == 'video':
                            orig_num_of_vstreams += 1
                            # Get only for first stream
                            if orig_num_of_vstreams == 1:
                                log.debug('extracting vcodec attributes')
                                orig_vcodec_name = codec
                                try:
                                    orig_vcodec_profile = stream['profile']
                                    orig_vcodec_width = stream['width']
                                    orig_vcodec_height = stream['height']
                                    orig_vcodec_aspect_ratio = stream['display_aspect_ratio']
                                    orig_vcodec_pix_fmt = stream['pix_fmt']
                                    orig_vcodec_level = stream['level']
                                except Exception as e:
                                    log.warning('some vcodec attributes might not be available')
                                    log.warning(e)
                                else:
                                    pass
                        else:
                            orig_num_of_astreams += 1
                            # Get only for first stream
                            if orig_num_of_astreams == 1:
                                log.debug('extracting acodec attributes')
                                orig_acodec_name = codec
                                try:
                                    orig_acodec_sample_fmt = stream['sample_fmt']
                                    orig_acodec_sample_rate = stream['sample_rate']
                                    orig_acodec_channels = stream['channels']
                                    orig_acodec_channel_layout = stream['channel_layout']
                                    orig_acodec_bit_rate = stream['bit_rate']
                                except Exception as e:
                                    log.warning('some acodec attributes might not be available')
                                    log.warning(e)
                                else:
                                    pass

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
                        else:
                            log.info('found mjpeg, ignoring')
                    elif stream_type == 'subtitle':
                        orig_num_of_sstreams += 1
                    else:
                        orig_num_of_ostreams += 1

                vcodec = 'copy'
                acodec = 'copy'
                complete_flag = False
                ts_complete = None
                no_conversion_required = False

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
                    'ts_complete': ts_complete,
                    'orig_fsize': movie['orig_fsize'],
                    'orig_format': movie_info['format']['format_name'],
                    'orig_num_of_streams': orig_num_of_streams,
                    'orig_num_of_astreams': orig_num_of_astreams,
                    'orig_num_of_vstreams': orig_num_of_vstreams,
                    'orig_num_of_sstreams': orig_num_of_sstreams,
                    'orig_num_of_ostreams': orig_num_of_ostreams,
                    'orig_vcodec_name': orig_vcodec_name,
                    'orig_vcodec_profile': orig_vcodec_profile,
                    'orig_vcodec_width': orig_vcodec_width,
                    'orig_vcodec_height': orig_vcodec_height,
                    'orig_vcodec_aspect_ratio': orig_vcodec_aspect_ratio,
                    'orig_vcodec_pix_fmt': orig_vcodec_pix_fmt,
                    'orig_vcodec_level': orig_vcodec_level,
                    'orig_acodec_name': orig_acodec_name,
                    'orig_acodec_sample_fmt': orig_acodec_sample_fmt,
                    'orig_acodec_sample_rate': orig_acodec_sample_rate,
                    'orig_acodec_channels': orig_acodec_channels,
                    'orig_acodec_channel_layout': orig_acodec_channel_layout,
                    'orig_acodec_bit_rate': orig_acodec_bit_rate,
                    'ffprobe_metadata': ffprobe
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

