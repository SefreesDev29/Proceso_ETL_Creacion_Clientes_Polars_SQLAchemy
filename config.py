from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import computed_field

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    BD_IP: str
    BD_NAME: str
    
    BD_USER_DEV_1: str
    BD_PASSWORD_DEV_1: str
    
    BD_USER_DEV_2: str | None = None
    BD_PASSWORD_DEV_2: str | None = None

    @computed_field
    def database_url(self) -> str:
        return f"mssql+pyodbc://{self.BD_USER_DEV_1}:{self.BD_PASSWORD_DEV_1}@{self.BD_IP}/{self.BD_NAME}?driver=ODBC+Driver+17+for+SQL+Server"

settings = Settings()