import os
from dotenv import load_dotenv

def get_secrets() -> str:
  """Loads DATABASE_URL from environment variables."""
  # TODO: Replace with a proper secret manager for production deployments.
  load_dotenv()
  database_url = os.getenv('DATABASE_URL')
  if not database_url:
    raise Exception("DATABASE_URL not found in environment variables.")
  return database_url