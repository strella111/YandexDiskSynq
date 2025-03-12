from datetime import datetime
import time
import os
from dotenv import dotenv_values, load_dotenv

from loguru import logger

from cloud_disk import CloudDisk


def main(period):

    config = dotenv_values()
    load_dotenv()
    auth_token = config.get('token')
    local_path = config.get('local_path')
    cloud_path = config.get('cloud_path')

    ya_disk = CloudDisk(token=auth_token, dir=cloud_path)

    logger.info(f'Старт синхронизации папки {local_path} с папкой в облаке {cloud_path}')
    while True:
        try:
            cloud_files = ya_disk.get_info()
            local_files = os.listdir(local_path)
            for file in local_files:
                if file not in cloud_files:
                    ya_disk.load(filepath=os.path.join(local_path, file), filename=file)
                else:
                    local_last_mod = datetime.fromtimestamp(os.path.getmtime(os.path.join(local_path, file)))
                    cloud_last_mod = datetime.strptime(cloud_files[file], '%Y-%m-%dT%H:%M:%S%z' ).astimezone().replace(tzinfo=None)
                    if local_last_mod > cloud_last_mod:
                        ya_disk.reload(filepath=os.path.join(local_path, file), filename=file)

            for filename in cloud_files.keys():
                if filename not in local_files:
                    ya_disk.delete(filename)

        except (OSError, IOError) as e:
            logger.warning(f'Ошибка чтения файла: {e}')
        except Exception as e:
            logger.error(f'Непредвиденная ошибка {e}')

        time.sleep(period)



if __name__ == '__main__':
    logger.add('log.log', level="DEBUG")
    main(period=1)

