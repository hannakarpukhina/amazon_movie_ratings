from scripts.download_content_service import DownloadContentService as dcs
from scripts.gcs_service import GCSService


def download_to_gcs(src_url, bucket_name, file_path):
    GCSService.save_content_to_file(dcs.get_content(src_url), bucket_name, file_path)
