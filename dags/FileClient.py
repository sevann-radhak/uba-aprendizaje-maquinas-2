import os
import requests

class FileClient:
    def download_file(self, url):
        response = requests.get(url)
        response.raise_for_status()

        return response.content
                
    # def download_file(self, url, sub_dir='', filename=''):
    #     if sub_dir:
    #         os.makedirs(os.path.join(self.base_dir, sub_dir), exist_ok=True)

    #     file_path = os.path.join(self.base_dir, sub_dir, filename)

    #     response = requests.get(url, stream=True)
    #     response.raise_for_status()
    #     with open(file_path, 'wb') as file:
    #         for chunk in response.iter_content(chunk_size=8192):
    #             file.write(chunk)