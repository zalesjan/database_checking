import streamlit as st
import io
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential

# SharePoint Credentials
SHAREPOINT_URL = st.secrets["MS"]["SHAREPOINT_URL"]
CLIENT_ID = st.secrets["MS"]["SHAREPOINT_URL"]
CLIENT_SECRET = st.secrets["MS"]["SHAREPOINT_URL"]

# SharePoint Folder Path (inside 'Documents' or another library)
SHAREPOINT_FOLDER = "Shared Documents/General/Harmonised"

# Authenticate to SharePoint
ctx = ClientContext(SHAREPOINT_URL).with_credentials(ClientCredential(CLIENT_ID, CLIENT_SECRET))

def fetch_txt_files_from_sharepoint():
    """ Fetches all .txt files from a SharePoint folder and returns them as a dictionary. """
    file_dict = {}
    folder = ctx.web.get_folder_by_server_relative_url(SHAREPOINT_FOLDER).files
    ctx.load(folder)
    ctx.execute_query()

    for file in folder:
        if file.name.endswith(".txt"):  # Filter .txt files only
            file_content = file.open_binary(ctx).content
            file_dict[file.name] = io.BytesIO(file_content)  # Store file in memory
            print(f"Fetched: {file.name}")

    return file_dict

# Streamlit UI
st.title("Upload SharePoint .txt Files to Streamlit")

if st.button("Fetch .txt Files from SharePoint"):
    txt_files = fetch_txt_files_from_sharepoint()

    if txt_files:
        st.success(f"Found {len(txt_files)} .txt files.")
        
        for file_name, file_content in txt_files.items():
            st.write(f"**{file_name}**")
            st.download_button(f"Download {file_name}", file_content, file_name)
    else:
        st.warning("No .txt files found in the SharePoint folder.")
