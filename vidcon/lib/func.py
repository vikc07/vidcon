"""
Common functions
"""

from sqlalchemy import *
import os
import subprocess
import smtplib
import time
import email
from email.mime.text import MIMEText
import email.utils
from datetime import datetime
from gpm import config, logging


cfg = None
log = None


def get_file_path(file):
    if os.path.isfile(file):
        file = os.path.abspath(file)

    return os.path.dirname(file)


def read_cfg(script):
    global cfg
    cfg = config.Config(script=script, cfg_file='vidcon.json')
    cfg.read()
    return cfg


def init_log(script):
    global log
    log = logging.Log(log_level=cfg.LOG_LEVEL, script=script, tsformat=cfg.LOG_TSFORMAT, log_entry_format_separator=' ')
    return log


conn = None


def init_queue():
    global conn
    dbconn = "{driver}://{user}:{passwd}@{host}:{port}/{dbname}?charset={charset}"
    dbconn = dbconn.format(driver=cfg.DB['DRIVER'], host=cfg.DB['HOST'], port=cfg.DB['PORT'], dbname=cfg.DB['DBNAME'],
                  charset=cfg.DB['CHARSET'], user=cfg.DB['USER'], passwd=cfg.DB['PASS'])
    engine = create_engine(dbconn)
    if conn is None:
        conn = engine.connect()
    queue = Table(cfg.DB_TBL_VIDCON_QUEUE, MetaData(engine), autoload=True)

    return queue


def close_queue():
    conn.close()


def get_all_files_in_queue():
    queue = init_queue()
    sql = select([queue.c.input_file, queue.c.complete_flag, queue.c.ts_complete,
                  queue.c.ts_modified, queue.c.id]).order_by(
        queue.c.ts_added)
    result = conn.execute(sql)
    rows = result.fetchall()
    movies = {}
    for row in rows:
        movies[row['input_file']] = {}
        movies[row['input_file']]['id'] = row['id']
        movies[row['input_file']]['ts_complete'] = row['ts_complete']
        movies[row['input_file']]['complete_flag'] = row['complete_flag']
        movies[row['input_file']]['ts_modified'] = row['ts_modified']
    return movies


def get_all_incomplete_files_in_queue():
    queue = init_queue()
    sql = select([queue]).order_by(queue.c.ts_added).where(queue.c.complete_flag != 1)
    result = conn.execute(sql)
    rows = result.fetchall()
    return rows


def add_to_queue(movie):
    queue = init_queue()
    result = conn.execute(queue.insert().values(movie))
    return result.inserted_primary_key


def update_metadata_in_queue(movie):
    queue = init_queue()
    movie['ts_modified'] = datetime.now()
    id = movie['id']
    movie.pop('id')
    return conn.execute(queue.update().where(queue.c.id == id).values(movie))


def mark_completed_in_queue(id):
    queue = init_queue()
    return conn.execute(queue.update().where(queue.c.id == id).values(complete_flag=1, ts_complete=datetime.now()))


def ls(path, recursive=False):
    files = [os.path.join(path, file) for file in os.listdir(path)]
    if recursive:
        for file in files:
            if os.path.isdir(file):
                files = files + ls(file, recursive)

    return files


def get_files():
    files = []
    for folder in cfg.VIDCON_MONITOR_FOLDER:
        files = files + ls(folder, recursive=cfg.VIDCON_MONITOR_FOLDER_RECURSIVE)

    return files


def get_file_extension(file):
    return os.path.splitext(file)[-1].lower()


def get_file_name_without_extension(file):
    return os.path.splitext(os.path.basename(file))[0]


def call_process(cmd):
    log.debug('executing {cmd}'.format(cmd=' '.join(cmd)))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy())
    # surrogateescape fixes the decoding errors otherwise
    stdout, stderr = [x.decode('utf-8', errors='surrogateescape') for x in proc.communicate()]

    return {'returncode': proc.returncode, 'stdout': stdout + stderr}


def ffprobe(file):
    ffprobe_cmd = [
        cfg.CMD_FFPROBE,
        '-show_streams',
        '-show_format',
        '-print_format',
        'json',
        '-v',
        'quiet',
        file
    ]
    out = call_process(ffprobe_cmd)
    if out['returncode'] == 0:
        if len(out['stdout']) > 0:
            return True, out['stdout']
        else:
            return False, out['stdout']
    else:
        return False, out['stdout']


def ffmpeg(input_file, title, vcodec, acodec, output_file):
    acodec_list = acodec.split(' ')
    vcodec_list = vcodec.split(' ')
    ffmpeg_cmd = [
                     cfg.CMD_FFMPEG,
                     '-y',
                     '-i',
                     input_file,
                     '-fflags',
                     '+genpts',
                     '-map',
                     '0',
                     '-map',
                     '-0:d',
                     '-map',
                     '-0:v:1',
                     '-metadata:s:v:0',
                     'title=' + title,
                     '-c:v'
                 ] + vcodec_list + \
                 [
                     '-c:s',
                     'copy',
                     '-c:a'
                 ] + acodec_list + \
                 [
                     output_file
                 ]
    out = call_process(ffmpeg_cmd)
    if out['returncode'] == 0:
        if len(out['stdout']) > 0:
            return True, out['stdout']
        else:
            return False, out['stdout']
    else:
        return False, out['stdout']


def _lock_file(file):
    return os.path.join(get_file_path(file), get_file_name_without_extension(file) + '.lock')


def quit_if_already_running(file):
    lock_file = _lock_file(file)
    if os.path.exists(lock_file):
        print('already running')
        exit(0)


def create_lock_file(file):
    lock_file = _lock_file(file)
    with open(lock_file, 'a'):
        os.utime(lock_file, None)


def remove_lock_file(file):
    lock_file = _lock_file(file)
    os.remove(lock_file)


def sendalert(subject, text):
    text = text + "\n\nTime: " + time.strftime('%b %d, %Y %I:%M:%S %p')

    if cfg.ALERTS_ENABLED:
        # Construct the message
        msg = MIMEText(text)
        msg['Subject'] = subject
        msg['From'] = email.utils.formataddr((cfg.ALERTS_EMAIL_FROM_NAME, cfg.ALERTS_EMAIL_FROM_EMAIL))
        msg['To'] = email.utils.formataddr((cfg.ALERTS_EMAIL_TO_NAME, cfg.ALERTS_EMAIL_TO_EMAIL))

        # Send email
        server = smtplib.SMTP(host=cfg.ALERTS_EMAIL_HOST, port=cfg.ALERTS_EMAIL_PORT)
        # server.set_debuglevel(1)
        if cfg.ALERTS_EMAIL_TLS:
            server.starttls()

        if cfg.ALERTS_EMAIL_USER != "" and cfg.ALERTS_EMAIL_PASS != "":
            server.login(cfg.ALERTS_EMAIL_USER, cfg.ALERTS_EMAIL_PASS)
        server.send_message(msg)
        server.quit()
    else:
        log.debug('alerts are disabled')
