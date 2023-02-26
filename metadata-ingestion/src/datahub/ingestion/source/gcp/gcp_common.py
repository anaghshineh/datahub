import json
import logging
import os
import tempfile
from typing import Any, Dict, Optional

from pydantic import Field, PrivateAttr, root_validator

from datahub.configuration import ConfigModel
from datahub.configuration.source_common import (
    PlatformSourceConfigBase,
)

logger = logging.getLogger(__name__)


class GCPServiceAccountKey(ConfigModel):
    project_id: str = Field(description="Project id to set the credentials")
    private_key_id: str = Field(description="Private key id")
    private_key: str = Field(
        description="Private key in a form of '-----BEGIN PRIVATE KEY-----\\nprivate-key\\n-----END PRIVATE KEY-----\\n'"
    )
    client_email: str = Field(description="Client email")
    client_id: str = Field(description="Client Id")
    auth_uri: str = Field(
        default="https://accounts.google.com/o/oauth2/auth",
        description="Authentication uri",
    )
    token_uri: str = Field(
        default="https://oauth2.googleapis.com/token", description="Token uri"
    )
    auth_provider_x509_cert_url: str = Field(
        default="https://www.googleapis.com/oauth2/v1/certs",
        description="Auth provider x509 certificate url",
    )
    type: str = Field(
        default="service_account", description="Authentication type"
    )
    client_x509_cert_url: Optional[str] = Field(
        default=None,
        description="If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email",
    )

    @root_validator()
    def validate_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("client_x509_cert_url") is None:
            values[
                "client_x509_cert_url"
            ] = f'https://www.googleapis.com/robot/v1/metadata/x509/{values["client_email"]}'
        return values

    def create_credential_temp_file(self) -> str:
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            cred_json = json.dumps(self.dict(), indent=4, separators=(",", ": "))
            fp.write(cred_json.encode())
            return fp.name

class GCPSourceConfig(PlatformSourceConfigBase):
    credential: Optional[GCPServiceAccountKey] = Field(
        description="GCP service account credentials"
    )
    _credentials_path: Optional[str] = PrivateAttr(None)

    def __init__(self, **data: Any):
        super().__init__(**data)

        if self.credential:
            self._credentials_path = self.credential.create_credential_temp_file()
            logger.debug(
                f"Creating temporary credential file at {self._credentials_path}"
            )
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self._credentials_path

