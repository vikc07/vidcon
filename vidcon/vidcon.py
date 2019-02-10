"""
Video Converter

todo: after successful completion perform cleanup
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

    # Counts
    converted_count = 0
    converted_movies = []
    not_converted_count = 0
    not_converted_movies = []

    # Process the queue
    temp_folder = cfg.VIDCON_TEMP_FOLDER

    cleanup_needed = False

    queue = func.get_all_incomplete_files_in_queue()
    for item in queue:
        id = item['id']
        title = item['title']
        input_file = item['input_file']
        output_file = item['output_file']
        temp_output_file = os.path.join(temp_folder, os.path.basename(output_file))
        vcodec = item['vcodec']
        acodec = item['acodec']
        log.info('title: ' + title)
        log.info('input file: ' + input_file)
        log.info('output file: ' + output_file)
        log.info('audio codec: ' + acodec)
        log.info('video codec: ' + vcodec)

        # Run ffmpeg
        try:
            ffmpeg_success, ffmpeg = func.ffmpeg(input_file=input_file, output_file=temp_output_file, title=title,
                                                   acodec=acodec, vcodec=vcodec)
            log.debug('ffmpeg output ' + ffmpeg)
        except Exception as e:
            log.error('error occcurred running ffmpeg')
            log.error(e)
            err = 1
        else:
            if ffmpeg_success:
                log.info('conversion done')
                converted_count = converted_count + 1
                converted_movies.append(title)

                if cfg.REMOVE_SRC_FILE:
                    # Remove original file
                    log.info('removing input file')
                    try:
                        os.remove(input_file)
                    except IOError as e:
                        log.warning('could not remove input file')
                        log.warning(e)
                        cleanup_needed = True
                    else:
                        log.info('input file removed')

                log.info('moving converted file to final location')
                try:
                    output_folder = os.path.dirname(output_file)
                    if not os.path.exists(output_folder):
                        log.info('output folder {folder} does not exist, creating it'.format(folder=output_folder))
                        if os.mkdir(output_folder):
                            log.info('output folder created')
                        else:
                            log.error('could not create output folder')

                    shutil.move(temp_output_file, output_file)
                except IOError as e:
                    log.error('could not move to destination ')
                    log.error(e)
                    err = 1
                else:
                    log.info('successfully moved')
                    log.info('updating queue')

                    if func.mark_completed_in_queue(id):
                        log.info('queue updated')
                    else:
                        log.error('error updating queue')

            else:
                log.error('error running ffmpeg')
                not_converted_count = not_converted_count + 1
                not_converted_movies.append(title)

    if len(queue) > 0:
        subject = 'Video Convertor update'
        msg = ''
        if converted_count > 0:
            msg = msg + 'Successfully processed following movies:\n'
            for movie in converted_movies:
                msg = msg + movie + '\n'

        if not_converted_count > 0:
            msg = msg + 'Could not process following movies:\n'
            for movie in not_converted_movies:
                msg = msg + movie + '\n'

        if cleanup_needed:
            msg = msg + "\nYou will need to clean up downloads folder."

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
