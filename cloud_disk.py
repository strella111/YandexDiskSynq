import requests
from loguru import logger

class CloudDisk:

    def __init__(self, token, dir):
        self.dir = dir
        self.base_url = 'https://cloud-api.yandex.net/v1/disk/'
        self.headers = {
            'Authorization': f'OAuth {token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def _parse_get_info(self, response: requests.Response):
        items = response.json()['_embedded']['items']
        result = {item['path'].replace(self.dir + '/', ''): item['modified'] for item in items}
        return result


    def _get_link_to_upload(self, path, overwrite):
        url = self.base_url + 'resources/upload'
        params = {
            'path': path,
            'overwrite': overwrite
        }
        response = requests.get(url=url, headers=self.headers, params=params)
        if response.ok:
            return response.json()['href']
        else:
            logger.error(f'Ошибка запроса ссылки на загрузку файла на диск: {response.json()['message']}')
            return None

    def load(self, filename, filepath, overwrite=False):
        url = self._get_link_to_upload(self.dir + '/' + filename, overwrite)
        if url:
            response = requests.put(url=url, data=open(filepath, 'rb'))
            if not response.ok:
                logger.error(f'Ошибка загрузки файла {filepath} на диск: {response.json()['message']}')
            if overwrite:
                logger.info(f'Обновлен файл {filename} на диск')
            else:
                logger.info(f'Загружен файл {filename} на диск')


    def reload(self, filename, filepath):
        self.load(filename, filepath, overwrite=True)

    def delete(self, filename):
        url = self.base_url + 'resources'
        params = {
            'path': self.dir + '/' + filename
        }
        response = requests.delete(url=url, params=params, headers=self.headers)
        if not response.ok:
            logger.error(f'Ошибка удаления файла {filename}: {response.json()['message']}')
        return logger.info(f'Удален файл {filename} с диска')

    def get_info(self):
        endpoint = self.base_url + 'resources'
        params = {
            'path': self.dir
        }
        response = requests.get(url=endpoint, headers=self.headers, params=params)
        if response.ok:
            return self._parse_get_info(response)
        else:
            logger.error(f'Ошибка чтения матеданных директории: {response.json()['message']}')
            return response.json()



