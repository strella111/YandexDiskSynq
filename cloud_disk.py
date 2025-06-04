import requests
from loguru import logger


class CloudDiskError(Exception):
    """Базовый класс для ошибок CloudDisk"""
    pass


class CloudDiskAPIError(CloudDiskError):
    """Ошибка при взаимодействии с API Яндекс.Диска"""
    pass


class CloudDiskConfigError(CloudDiskError):
    """Ошибка конфигурации"""
    pass


class CloudDisk:

    def __init__(self, token, dir):
        if not token:
            raise CloudDiskConfigError("Токен авторизации не указан")
        if not dir:
            raise CloudDiskConfigError("Путь к директории в облаке не указан")
            
        self.dir = dir
        self.base_url = 'https://cloud-api.yandex.net/v1/disk/'
        self.headers = {
            'Authorization': f'OAuth {token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def _parse_get_info(self, response: requests.Response):
        try:
            items = response.json()['_embedded']['items']
            result = {item['path'].replace(self.dir + '/', ''): item['modified'] for item in items}
            return result
        except (KeyError, ValueError) as e:
            raise CloudDiskAPIError(f"Ошибка при разборе ответа API: {str(e)}")

    def _get_link_to_upload(self, path, overwrite):
        url = self.base_url + 'resources/upload'
        params = {
            'path': path,
            'overwrite': overwrite
        }
        try:
            response = requests.get(url=url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()['href']
        except requests.exceptions.RequestException as e:
            error_msg = f'Ошибка запроса ссылки на загрузку файла на диск: {str(e)}'
            logger.error(error_msg)
            raise CloudDiskAPIError(error_msg)

    def load(self, filename, filepath, overwrite=False):
        try:
            url = self._get_link_to_upload(self.dir + '/' + filename, overwrite)
            if not url:
                raise CloudDiskAPIError("Не удалось получить ссылку для загрузки")
                
            with open(filepath, 'rb') as file:
                response = requests.put(url=url, data=file)
                response.raise_for_status()
                
            if overwrite:
                logger.info(f'Обновлен файл {filename} на диск')
            else:
                logger.info(f'Загружен файл {filename} на диск')
        except (OSError, IOError) as e:
            error_msg = f'Ошибка при работе с файлом {filepath}: {str(e)}'
            logger.error(error_msg)
            raise CloudDiskError(error_msg)
        except requests.exceptions.RequestException as e:
            error_msg = f'Ошибка загрузки файла {filepath} на диск: {str(e)}'
            logger.error(error_msg)
            raise CloudDiskAPIError(error_msg)

    def reload(self, filename, filepath):
        self.load(filename, filepath, overwrite=True)

    def delete(self, filename):
        url = self.base_url + 'resources'
        params = {
            'path': self.dir + '/' + filename
        }
        try:
            response = requests.delete(url=url, params=params, headers=self.headers)
            response.raise_for_status()
            logger.info(f'Удален файл {filename} с диска')
        except requests.exceptions.RequestException as e:
            error_msg = f'Ошибка удаления файла {filename}: {str(e)}'
            logger.error(error_msg)
            raise CloudDiskAPIError(error_msg)

    def get_info(self):
        endpoint = self.base_url + 'resources'
        params = {
            'path': self.dir
        }
        try:
            response = requests.get(url=endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            return self._parse_get_info(response)
        except requests.exceptions.RequestException as e:
            error_msg = f'Ошибка чтения метаданных директории: {str(e)}'
            logger.error(error_msg)
            raise CloudDiskAPIError(error_msg)



