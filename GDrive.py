import io
import re

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import functions as objects

standard_file_fields = 'files(id, name, parents, createdTime, modifiedTime)'


class Drive:
    def __init__(self, credentials_dict):
        scope = ['https://www.googleapis.com/auth/drive']
        credentials = service_account.Credentials.from_service_account_info(credentials_dict, scopes=scope)
        self.client = build('drive', 'v3', credentials=credentials)

    @staticmethod
    def revoke_time(file):
        for key in ['modifiedTime', 'createdTime']:
            if file.get(key):
                stamp = re.sub(r'\..*?Z', '', file[key])
                file[key] = objects.stamper(stamp, pattern='%Y-%m-%dT%H:%M:%S')
        return file

    def download_file(self, file_id, file_path):
        done = False
        file = io.FileIO(file_path, 'wb')
        downloader = MediaIoBaseDownload(file, self.client.files().get_media(fileId=file_id))
        while done is False:
            try:
                status, done = downloader.next_chunk()
            except IndexError and Exception:
                done = False

    def files(self, fields=standard_file_fields, only_folders=False, name_startswith=False, parents=False):
        query = ''
        response = []
        if only_folders:
            query = "mimeType='application/vnd.google-apps.folder'"
        if name_startswith:
            if query:
                query += ' and '
            query += f"name contains '{name_startswith}'"
        if parents:
            if query:
                query += ' and '
            query += f"'{parents}' in parents"
        result = self.client.files().list(q=query, pageSize=1000, fields=fields).execute()
        for file in result['files']:
            response.append(self.revoke_time(file))
        return response
