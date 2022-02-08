import os
from typing import Dict

import requests

ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "general")
PROTOCOL = os.environ.get("PROTOCOL", "http")
HOST = os.environ.get("HOST", "localhost")
PORT = os.environ.get("PORT", 8088)
SUPERSET_URL = "{protocol}://{host}:{port}".format(
    protocol=PROTOCOL, host=HOST, port=PORT
)
LOGIN_URI = "/api/v1/security/login"
CSRF_URI = "/api/v1/security/csrf_token/"
DATABASE_IMPORT = "/api/v1/database/import/"
DATASET_API = "/api/v1/dataset"
DATASET_IMPORT = DATASET_API + "/import/"
CSRF_TOKEN_HEADER = "X-CSRFToken"
DATABASE_FILE_PATH = "zip_db.zip"
DATASET_FILE_PATH = "build.zip"

HEADER_LOGIN_PARAMS = {"Accept": "application/json", "Content-Type": "application/json"}


class SupersetClient:
    header_auth_param: Dict[str, str]

    def __init__(
        self, username: str = ADMIN_USERNAME, password: str = ADMIN_PASSWORD
    ) -> None:
        self.header_auth_param = self.build_auth_param_headers(username, password)

    def set_csrf(self) -> None:
        self.header_auth_param[CSRF_TOKEN_HEADER] = self.get_csrf_token()

    def build_auth_param_headers(self, username: str, password: str) -> Dict[str, str]:
        access_token = self.get_access_token(username, password)
        return {
            "Accept": "application/json",
            "Authorization": "Bearer {}".format(access_token),
        }

    def get_access_token(self, username: str, password: str) -> str:
        access_token_request = requests.post(
            SUPERSET_URL + LOGIN_URI,
            json={"username": username, "password": password, "provider": "db"},
            headers=HEADER_LOGIN_PARAMS,
        )
        return access_token_request.json()["access_token"]

    def get_csrf_token(self) -> str:
        return str(
            requests.get(
                SUPERSET_URL + CSRF_URI,
                params=HEADER_LOGIN_PARAMS,
                headers=self.build_auth_param_headers,
            )
            .json()
            .get("result")
        )

    def import_database_file(self, file_path: str) -> requests.Response:
        return self.import_file(file_path, DATABASE_IMPORT)

    def import_dataset_file(self, file_path: str) -> requests.Response:
        return self.import_file(file_path, DATASET_IMPORT)

    def import_file(self, file_path: str, uri: str) -> requests.Response:
        url = SUPERSET_URL + uri
        files_database = {
            "formData": (file_path, open(file_path, "rb"), "application/zip")
        }
        return requests.post(url, files=files_database, headers=self.header_auth_param)

    def get_dataset(self) -> requests.Response:
        return requests.get(SUPERSET_URL + DATASET_API, headers=self.header_auth_param)


def main() -> None:
    superset_client = SupersetClient(username=ADMIN_USERNAME, password=ADMIN_PASSWORD)
    import_database_response = superset_client.import_database_file(DATABASE_FILE_PATH)
    import_dataset_response = superset_client.import_dataset_file(DATASET_FILE_PATH)
    datasets_response = superset_client.get_dataset()


if __name__ == "__main__":
    main()
