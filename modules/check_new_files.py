from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
import schedule
import time
import os

# SharePoint credentials
sharepoint_url = "https://your-sharepoint-site-url"
client_id = "your-client-id"
client_secret = "your-client-secret"
site_url = "your-site-url"
folder_path = "your/folder/path"

# Authenticate
ctx = ClientContext(sharepoint_url).with_credentials(ClientCredential(client_id, client_secret))

def check_new_files():
    folder = ctx.web.get_folder_by_server_relative_url(folder_path).expand(["Files"])
    ctx.load(folder)
    ctx.execute_query()
    files = folder.files

    for file in files:
        local_path = os.path.join("/local/download/path", file.name)
        with open(local_path, "wb") as local_file:
            file.download(local_file).execute_query()
        print(f"Downloaded: {file.name}")
        validate_file(local_path)

schedule.every(5).minutes.do(check_new_files)

while True:
    schedule.run_pending()
    time.sleep(1)
