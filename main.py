from datetime import datetime
import time
import os
from dotenv import dotenv_values, load_dotenv
import configparser

from loguru import logger

from cloud_disk import CloudDisk, CloudDiskConfigError


def validate_config(config):
    """Проверка конфигурации
    config - конфигурация
    """
    required_fields = ['token', 'local_path', 'cloud_path', 'period', 'log_path']
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        raise CloudDiskConfigError(f"Отсутствуют обязательные поля в конфигурации: {', '.join(missing_fields)}")
    
    if not os.path.exists(config['local_path']):
        raise CloudDiskConfigError(f"Локальная директория {config['local_path']} не существует")


def sync_files(ya_disk, local_path):
    """Синхронизация файлов
    ya_disk - объект класса CloudDisk
    local_path - путь к локальной папке
    """
    try:
        cloud_files = ya_disk.get_info()
        local_files = os.listdir(local_path)
        
        for file in local_files:
            try:
                if file not in cloud_files:
                    ya_disk.load(filepath=os.path.join(local_path, file), filename=file)
                else:
                    local_last_mod = datetime.fromtimestamp(os.path.getmtime(os.path.join(local_path, file)))
                    cloud_last_mod = datetime.strptime(cloud_files[file], '%Y-%m-%dT%H:%M:%S%z').astimezone().replace(tzinfo=None)
                    if local_last_mod > cloud_last_mod:
                        ya_disk.reload(filepath=os.path.join(local_path, file), filename=file)
            except Exception as e:
                logger.error(f'Ошибка при обработке файла {file}: {str(e)}')
                continue

        for filename in cloud_files.keys():
            try:
                if filename not in local_files:
                    ya_disk.delete(filename)
            except Exception as e:
                logger.error(f'Ошибка при удалении файла {filename}: {str(e)}')
                continue
                
    except Exception as e:
        logger.error(f'Ошибка при синхронизации: {str(e)}')


def main(auth_token, local_path, cloud_path, period):
    """Основная функция программы
    auth_token - токен авторизации
    local_path - путь к локальной папке
    cloud_path - путь к облачной папке
    period - период синхронизации
    """
    try:
        validate_config(config)

        ya_disk = CloudDisk(token=auth_token, dir=cloud_path)

        logger.info(f'Старт синхронизации папки {local_path} с папкой в облаке {cloud_path}. Период синхронизации: {period} секунд')
        
        while True:
            try:
                sync_files(ya_disk, local_path)
            except Exception as e:
                logger.error(f'Ошибка в цикле синхронизации: {str(e)} (работа продолжается)')
            finally:
                time.sleep(period)
                
    except CloudDiskConfigError as e:
        logger.error(f'Ошибка конфигурации: {str(e)}')
    except Exception as e:
        logger.error(f'Критическая ошибка: {str(e)}')


if __name__ == '__main__':

    try:
        config = configparser.ConfigParser()
        config.read('config.ini', encoding='utf-8')

        if not config['DEFAULT']['token']:
            raise CloudDiskConfigError('Токен авторизации не указан')
        if not config['DEFAULT']['local_path']:
            raise CloudDiskConfigError('Путь к локальной папке не указан')
        if not config['DEFAULT']['cloud_path']:
            raise CloudDiskConfigError('Путь к облачной папке не указан')
        if not config['DEFAULT']['period']:
            raise CloudDiskConfigError('Период синхронизации не указан')
        
        auth_token = config['DEFAULT']['token']
        local_path = config['DEFAULT']['local_path']
        cloud_path = config['DEFAULT']['cloud_path']
        period = int(config['DEFAULT']['period'])
        log_path = config['DEFAULT']['log_path']

        logger.add(log_path, level="DEBUG", rotation="1 day", retention="7 days")


        main(auth_token, local_path, cloud_path, period)

    except Exception as e:
        logger.error(f'Критическая ошибка: {str(e)}')

