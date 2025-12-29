import gspread
import pandas as pd
from google.oauth2.service_account import Credentials


def load_google_sheet(
    spreadsheet_id: str,
    worksheet_name: str,
    service_account_path: str
) -> pd.DataFrame:
    """
    Load Google Sheet as a raw grid DataFrame.
    No headers assumed.
    """

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    credentials = Credentials.from_service_account_file(
        service_account_path, scopes=scopes
    )

    client = gspread.authorize(credentials)
    sheet = client.open_by_key(spreadsheet_id)
    worksheet = sheet.worksheet(worksheet_name)

    values = worksheet.get_all_values()
    df = pd.DataFrame(values)

    return df
