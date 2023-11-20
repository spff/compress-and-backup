import os
import sys
import subprocess
import threading
import argparse
import pathlib

import inquirer
import boto3
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv

load_dotenv()
MB = 1024 * 1024


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('base')
    parser.add_argument('src', help='the relative path from base to be backup')
    args_config, _ = parser.parse_known_args()
    return args_config


def main():
    args_config = parse_args()
    envs = {
        k: os.getenv(k)
        for k in [
            'aws_access_key_id',
            'aws_secret_access_key',
            's3_base',
            '7z_bin',
        ]
    }
    s3 = boto3.session.Session(aws_access_key_id=envs['aws_access_key_id'], aws_secret_access_key=envs['aws_secret_access_key']).resource('s3')
    bucket_str, prefix = envs['s3_base'].split('/', 1)
    bucket = s3.Bucket(bucket_str)

    base = pathlib.Path(args_config.base)
    src = base / args_config.src
    print(
        base.absolute().resolve(),
        src.absolute().resolve(),
    )
    children = [x for x in src.iterdir() if (x.is_dir() or not x.name.endswith('.7z'))]
    questions = [
        inquirer.Checkbox(
            "interests",
            message="Select directories to backup",
            choices=children,
            default=children,
        ),
    ]

    answers = inquirer.prompt(questions)
    cloud_crcs = {}
    if not answers or not answers['interests']:
        print('cancelled')
        return
    print(f'looking for {prefix}{args_config.src}')
    for x in bucket.objects.filter(Prefix=f'{prefix}{args_config.src}', Delimiter='/'):
        if not x.key.endswith('.7z'):
            continue
        cloud_crcs[x.key] = bucket.Object(x.key).metadata.get('checksum-crc32')

    for x in answers['interests']:
        print(x, x.name)
        archive_path = x/'..'/f'{x.name}.7z'
        src_crc = crc_path(envs['7z_bin'], x, False, x.is_dir())

        object_key = f'{prefix}{args_config.src}{x.name}.7z'

        if object_key in cloud_crcs:
            if cloud_crcs[object_key] == src_crc:
                print('No changes, skip')
                continue
            else:
                print(f"Found change, dir: {src_crc}, s3: {cloud_crcs[object_key]}")

        should_compress = True
        if archive_path.exists():
            archive_crc = crc_path(envs['7z_bin'], archive_path, True, x.is_dir())
            if archive_crc == src_crc:
                print('Found archive, upload directly')
                should_compress = False
            else:
                print(f'Found archive but crc not match, dir: {src_crc}, archive: {archive_crc}, overwrite')

        if should_compress:
            compress(envs['7z_bin'], archive_path, x)
            archive_crc = crc_path(envs['7z_bin'], archive_path, True, x.is_dir())
            if archive_crc != src_crc:
                raise Exception(
                    "Something wrong, maybe the dir has been changed after the process started {}, {}".format(
                        src_crc,
                        archive_crc
                    )
                )

        upload(
            archive_path,
            bucket,
            object_key,
            metadata={'checksum-crc32': src_crc}
        )
        archive_path.unlink()


def crc_path(bin, path, is_archive, is_dir):

    kw = 'CRC32  for data and names:    ' if is_dir else 'CRC32  for data:              ' 
    args = [bin, 't', str(path.resolve().absolute()), '-scrcCRC32'] if is_archive else [bin, 'h', '-scrcCRC32', str(path.resolve().absolute())]
    ret = subprocess.run(args, capture_output=True)
    for line in ret.stdout.decode('utf-8', errors='ignore').replace('\r', '').split('\n'):
        if line.startswith(kw):
            return line.split(kw)[1]
    else:
        raise Exception('Unexpected 7z crc result')


def compress(bin, archive_path, dir_path):
    print('Compressing ...')
    print()
    ret = subprocess.run([
        bin,
        'a',
        '-m0=zstd',
        '-mx11',
        str(archive_path.resolve().absolute()),
        str(dir_path.resolve().absolute()),
    ], capture_output=True)
    if ret.returncode != 0:
        print()
        raise Exception(f"Invalid result: {ret.returncode}, {ret.stderr.decode('utf-8')}")


class TransferCallback:
    """
    Handle callbacks from the transfer manager.

    The transfer manager periodically calls the __call__ method throughout
    the upload and download process so that it can take action, such as
    displaying progress to the user and collecting data about the transfer.
    """

    def __init__(self, target_size):
        self._target_size = target_size
        self._total_transferred = 0
        self._lock = threading.Lock()
        self.thread_info = {}

    def __call__(self, bytes_transferred):
        """
        The callback method that is called by the transfer manager.

        Display progress during file transfer and collect per-thread transfer
        data. This method can be called by multiple threads, so shared instance
        data is protected by a thread lock.
        """
        thread = threading.current_thread()
        with self._lock:
            self._total_transferred += bytes_transferred
            if thread.ident not in self.thread_info.keys():
                self.thread_info[thread.ident] = bytes_transferred
            else:
                self.thread_info[thread.ident] += bytes_transferred

            target = self._target_size * MB
            sys.stdout.write(
                f"\r{self._total_transferred} of {target} transferred "
                f"({(self._total_transferred / target) * 100:.2f}%)."
            )
            sys.stdout.flush()


def upload(
    src,
    bucket,
    object_key,
    metadata=None,
    storage_class='DEEP_ARCHIVE',
):
    '''
      :param storage_class: 'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR'|'SNOW'
    '''
    transfer_callback = TransferCallback(src.stat().st_size)
    config = TransferConfig(multipart_threshold=20 * MB)
    extra_args = {}

    for k, v in [
        ('Metadata', metadata),
        ('StorageClass', storage_class), 
    ]:
        if v:
            extra_args[k] = v
    print(object_key)
    bucket.upload_file(
        src,
        object_key,
        Config=config,
        ExtraArgs=extra_args if extra_args else None,
        Callback=transfer_callback,
    )
    return transfer_callback.thread_info


def download(bucket, object_key, download_file_path, file_size_mb):
    transfer_callback = TransferCallback(file_size_mb)
    config = TransferConfig(multipart_threshold=20 * MB)
    bucket.Object(object_key).download_file(
        download_file_path, Config=config, Callback=transfer_callback
    )
    return transfer_callback.thread_info


try:
    main()
except Exception as e:
    import traceback
    for line in traceback.TracebackException.from_exception(e, capture_locals=True).format(chain=True):
        print(line, end="")
    exit(-1)
