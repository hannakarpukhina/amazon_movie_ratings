import requests

class DownloadContentService:
    def get_content(url):
        download = requests.get(url)
        if download.status_code == 404:
            raise Exception("Invalid URL")
        return download.content
