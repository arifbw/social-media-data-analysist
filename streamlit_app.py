import streamlit as st
import pandas as pd
import plotly.express as px
from PIL import Image
import re
import requests
from openpyxl import load_workbook
from stqdm import stqdm
from io import BytesIO
from datetime import datetime
from streamlit_condition_tree import condition_tree, config_from_dataframe
import time

# from streamlit_gsheets import GSheetsConnection

# Backend URLs
base_url = "https://api.kurasi.media"

# Login function to authenticate and retrieve tokens
def login(username, password):
    response = requests.post(base_url + "/login", json={"username": username, "password": password, "ref": "web", "remember_me": 0})
    
    if response.status_code == 200:
        tokens = response.json()

        st.session_state.user = tokens["user"]
        st.session_state.access_token = tokens['access-token']
        st.session_state.refresh_token = tokens['refresh-token']
        st.success("Logged in successfully!")
        st.rerun()
    else:
        st.error("Login failed. Please check your credentials.")


#fungsi-fungsi get data
@st.cache_data
def get_data_stats_all_medsos():
    response = requests.get(base_url + "/stats-source/456", headers={"token": st.session_state.access_token})

    if response.status_code == 200:
        return response.json()
    else: 
        return []

@st.cache_data
def get_data_stats_all_sentiment():
    response = requests.get(base_url + "/stats-sentiment/456", headers={"token": st.session_state.access_token})

    if response.status_code == 200:
        return response.json()
    else: 
        return []


@st.cache_data
def kalkulasi_banyak_row(uploaded_file):
    df = pd.read_excel(uploaded_file, sheet_name="Media Sosial")
    return len(df)

def classify_job_category(caption, categories):
        contains_others = bool(re.search("Lainnya", caption, re.IGNORECASE))

        found_categories = []
        for category in categories:
            if re.search(category, caption, re.IGNORECASE):
                found_categories.append(category)

        if found_categories:
            return found_categories[0]

        return 'Lainnya'

def classify_job_category2(caption, categories):
    matched_categories = []

    # Iterate through each category
    for category in categories["category_data"]:
        # Check if any threshold word is present in the caption (ignore case)
        for keyword in category["threshold"]:
            if re.search(r"\b" + re.escape(keyword) + r"\b", caption, re.IGNORECASE):
            # if re.search(r"(?<!\w)" + re.escape(keyword) + r"(?!\w)", caption, re.IGNORECASE):
            # if re.search(r"(?<![a-zA-Z])" + re.escape(keyword) + r"(?![a-zA-Z])", caption, re.IGNORECASE):
                matched_categories.append(category["name"])
                break  # Move to the next category once a match is found

    if not matched_categories:
        matched_categories.append("tdk_ada_informasi")

    if "tdk_ada_informasi" in matched_categories and len(matched_categories) > 1:
        matched_categories.remove("tdk_ada_informasi")

    return matched_categories

def process_chunk(chunk, kd):
    for item in stqdm(kd, desc="Analyze content semantic ...", backend=False, frontend=True):
        chunk[item["kamus_data"]] = chunk['Konten'].progress_apply(lambda x: classify_job_category2(str(x), item))
    return chunk

def clean_location_name(name):
    """
    Removes prefixes like 'Desa', 'Kecamatan', 'Kota', etc. and returns the cleaned name.
    """
    # Remove any prefix like Desa, Kota, Kecamatan, etc.
    prefixes = ['KOTA ', 'KAB. ', 'DAERAH ISTIMEWA ', 'KOTA ADM. ']
    for prefix in prefixes:
        if name.startswith(prefix):
            name = name.replace(prefix, "")
    return name.strip()

# Function to map location from Konten based on location data
def map_location(caption, location_df):
    """
    Finds the first matching location in the caption by comparing to the cleaned location names.
    """
    for location in location_df['cleaned_nama']:
        # Check if the location is found in the caption (ignore case)
        if re.search(location, caption, re.IGNORECASE):
            # If found, return the corresponding full location name
            return location_df[location_df['cleaned_nama'] == location]['nama'].values[0]
    # If no location is found, return 'Unknown'
    return 'Unknown'

job_categories = None
uploaded_file = None

keywords = None
def categorize_job_post(caption):
    caption = caption.lower()  # Convert caption to lowercase for case-insensitive matching
    caption = re.sub(r'[^\w\s]', '', caption)  # Remove punctuation

    for kd in job_categories:
        # Iterate over categories and their keywords
        for category in kd['category_data']:
            name = category['name']

            for keyword in category['threshold']:
                if keyword in caption:
                    return name

    return 'uncategorized'

def get_kamus_data():
    kamus_data_xls = pd.ExcelFile('kamus_data.xlsx')
    result = []
    
    for sheet_name in keywords:
        # Load the sheet into a DataFrame
        df = pd.read_excel(kamus_data_xls, sheet_name=sheet_name)

        # Remove columns that are completely empty (all NaN values)
        df = df.dropna(axis=1, how='all')
        
        # Remove rows that are completely empty (all NaN values)
        df = df.dropna(axis=0, how='all')
        
        
        # Initialize a list to hold column data for this sheet
        column_data = []

        # Iterate through each column in the DataFrame
        for column in df.columns:
            # Get the column data as a list
            list_data = df[column].dropna().tolist()
            
            # Add column name and list of data to column_data list
            if list_data:
                column_data.append({
                    "name": column,
                    "threshold": list_data
                })
            
        # Append the sheet data to result
        if column_data:
            result.append({
                "kamus_data": sheet_name,
                "category_data": column_data
            })

    return result

# Global session state for tokens
if 'access_token' not in st.session_state:
    st.session_state.access_token = None
if 'refresh_token' not in st.session_state:
    st.session_state.refresh_token = None
if 'token_thread_started' not in st.session_state:
    st.session_state['token_thread_started'] = False

def add_filter_component(df):
    with st.expander("Filter Data"):
        date_range = st.date_input("Select Tanggal Publikasi Range", [])
        topik_filter = st.multiselect("Select Topik", df['Topik'].unique())
        sentimen_filter = st.multiselect("Select Sentimen", df['Sentimen'].unique())
    
    # Filter by date (Tanggal Publikasi)
    if len(date_range) == 2:
        start_date, end_date = date_range
        df['Tanggal Publikasi'] = pd.to_datetime(df['Tanggal Publikasi'], errors='coerce')
        df = df[(df['Tanggal Publikasi'] >= pd.Timestamp(start_date)) & (df['Tanggal Publikasi'] <= pd.Timestamp(end_date))]

    # Filter by Topik
    if topik_filter:
        df = df[df['Topik'].isin(topik_filter)]

    # Filter by Sentimen
    if sentimen_filter:
        df = df[df['Sentimen'].isin(sentimen_filter)]

    return df

def setup_data_stats_and_sentiment(data, data2):
    st.sidebar.markdown(f"""
        <style>
            .custom-list li {{
                padding: 3px;
                font-size: 16px;
                border-bottom: 2px solid #cecece;
                list-style-type: none;
                margin-left: 0px;
            }}

            .title_sidebar{{
                color: #FF3333;
                font-weight: 900 !important;
            }}
                        
            iframe[title="streamlit_condition_tree.streamlit_condition_tree"]{{
                border: 3px #cccccc dashed;
                border-radius: 10px;
                padding: 20px 20px;
                box-sizing: content-box;
            }}
        </style>
        <h3 class="title_sidebar">Sosmed Data</h3>
        <ul class="custom-list">
            <li><i class="fa-brands fa-linkedin"></i> <strong>LinkedIn</strong>: {data['linkedin']:,} data</li>
            <li><i class="fa-brands fa-facebook"></i> <strong>Facebook</strong>: {data['facebook']:,} data</li>
            <li><i class="fa-brands fa-twitter"></i> <strong>Twitter</strong>: {data['twitter']:,} data</li>
            <li><i class="fa-brands fa-youtube"></i> <strong>YouTube</strong>: {data['youtube']:,} data</li>
            <li><i class="fa-brands fa-tiktok"></i> <strong>TikTok</strong>: {data['tiktok']:,} data</li>
            <li><i class="fa-brands fa-square-instagram"></i> <strong>Instagram</strong>: {data['instagram']:,} data</li>
        </ul>
    """, unsafe_allow_html=True)

    st.sidebar.markdown(f"""
        <h3 class="title_sidebar">Post Sentiment</h3>
        <ul class="custom-list">
            <li><i class="fa-solid fa-face-tired"></i> <strong>Negatif</strong>: {data2['negatif']:,} data</li>
            <li><i class="fa-solid fa-face-surprise"></i> <strong>Netral</strong>: {data2['netral']:,} data</li>
            <li><i class="fa-solid fa-face-smile"></i> <strong>Positif</strong>: {data2['positif']:,} data</li>
        </ul>
    """, unsafe_allow_html=True)

def generate_url(base_url, from_date, to_date, date_type="date", sort="date"):
    source = "ZmFjZWJvb2ssdHdpdHRlcixpbnN0YWdyYW0sdGlrdG9rLHlvdXR1YmUsZm9ydW0sYmxvZyxsaW5rZWRpbg=="
    now = datetime.now()
    cur_time = now.strftime("%Y%m%d%H%M%S")

    return f"{base_url}?from={from_date}%2000:00:00&to={to_date}%2023:59:59&date_type={date_type}&sort={sort}&sources={source}&time={cur_time}"

dynamic_url = None

@st.dialog("Download disini :")
def show_dynamic_url(url):
    st.write(f"{url}")

@st.dialog("Upload New Data :")
def update_kamus_data():
    data_baru = st.file_uploader("Upload Kamus Data :", type=["xlsx", "xls"])

    left, middle, right = st.columns(3)

    with right:
       if st.button("Lanjutkan", use_container_width=True, disabled=not data_baru):
           # Save the uploaded file to replace the existing one
            with open("kamus_data.xlsx", "wb") as f:
                f.write(data_baru.getbuffer())
                st.rerun()


# init page
st.set_page_config(
    page_title="Social Media Data Analysist",
    page_icon="📊",
    layout="wide"
)

img_logo = Image.open('logo_pasker.png')
st.logo(img_logo, size="large")


def eksekusi_excel2(df, kd):

    # for item in stqdm(kd, desc="Mengalisa data semantik konten ...", backend=False, frontend=True):
        # df[item["kamus_data"]] = df['Konten'].progress_apply(lambda x: classify_job_category2(str(x), item))

    formatted_array = [item["kamus_data"] for item in kd]
    st.dataframe(df[['Konten', 'Url'] + formatted_array])
    
    col1, col2 = st.columns(2, gap="large")

    with col1:
        for item in formatted_array:
            category_count = df[item].explode().value_counts().reset_index()
            category_count.columns = [item, 'Count']
            category_count_filtered = category_count[category_count[item] != 'tdk_ada_informasi']

            # Pie chart
            fig_pie_category = px.pie(category_count_filtered, names=item, values='Count', title=f'{item} Distribution in Captions')
            st.plotly_chart(fig_pie_category)

    with col2:
        for item in formatted_array:
            category_count = df[item].explode().value_counts().reset_index()
            category_count.columns = [item, 'Count']
            category_count_filtered = category_count[category_count[item] != 'tdk_ada_informasi']
            
            # Horizontal bar 
            fig_bar_category = px.bar(category_count_filtered, x='Count', y=item, orientation='h', title=f'{item} Distribution (Horizontal Bar Chart)')
            st.plotly_chart(fig_bar_category)


def eksekusi_excel(tab1, tab2, df):
    with tab1:
        with st.spinner('Memproses data, mohon tunggu sebentar ...'):
            df = add_filter_component(df)
            st.dataframe(df)

            col1, col2 = st.columns(2, gap="large")

            with col1:
                # Bar chart of Sentimen distribution
                st.write("### Sentimen Distribution")
                sentimen_count = df['Sentimen'].value_counts().reset_index()
                sentimen_count.columns = ['Sentimen', 'Count']
                fig_sentimen = px.bar(sentimen_count, x='Sentimen', y='Count', color='Sentimen')
                st.plotly_chart(fig_sentimen)

                st.write("### Engagement by Akun/Judul")
                engagement_by_account = df.groupby('Akun/Judul').agg({'Engagement':'sum'}).reset_index().sort_values(by='Engagement', ascending=False)
                fig_account = px.bar(engagement_by_account, x='Akun/Judul', y='Engagement', title='Top Accounts by Engagement')
                st.plotly_chart(fig_account)
            
            with col2:
                # Heatmap for Sentiment and Emotion
                st.write("### Sentiment vs Emotion Heatmap")
                heatmap_data = df.pivot_table(index='Sentimen', columns='Emotion', aggfunc='size', fill_value=0)
                fig_heatmap = px.imshow(heatmap_data, text_auto=True, title='Sentiment vs Emotion Heatmap')
                st.plotly_chart(fig_heatmap)

    with tab2:
        kd = get_kamus_data()

        for item in stqdm(kd, desc="Mengalisa data semantik konten ...", backend=False, frontend=True):
            df[item["kamus_data"]] = df['Konten'].progress_apply(lambda x: classify_job_category2(str(x), item))


        formatted_array = [item["kamus_data"] for item in kd]
        st.dataframe(df[['Konten', 'Url'] + formatted_array])
        
        col1, col2 = st.columns(2, gap="large")

        with col1:
            for item in formatted_array:
                category_count = df[item].explode().value_counts().reset_index()
                category_count.columns = [item, 'Count']
                category_count_filtered = category_count[category_count[item] != 'tdk_ada_informasi']

                # Pie chart
                fig_pie_category = px.pie(category_count_filtered, names=item, values='Count', title=f'{item} Distribution in Captions')
                st.plotly_chart(fig_pie_category)

        with col2:
            for item in formatted_array:
                category_count = df[item].explode().value_counts().reset_index()
                category_count.columns = [item, 'Count']
                category_count_filtered = category_count[category_count[item] != 'tdk_ada_informasi']
                
                # Horizontal bar 
                fig_bar_category = px.bar(category_count_filtered, x='Count', y=item, orientation='h', title=f'{item} Distribution (Horizontal Bar Chart)')
                st.plotly_chart(fig_bar_category)

if not st.session_state.access_token:
    st.markdown("""
        <style>
            .stForm{
                max-width: 400px;
                width: 100%;
                align-self: flex-end;
            }
            .stMainMenu{
                display: none !important;
            }
        </style>
    """, unsafe_allow_html=True)
    
    st.write("# Social Media Data Analysist 📈🚀")
    st.subheader("Dashboard for Insight & Semantic Data Analysist")

    col1, col2 = st.columns(2, vertical_alignment="center", gap="large")
    with col1:
        image_login = Image.open('login.png')
        st.image(image_login, width=400)
    with col2:
        with st.form("login_form"):
            st.subheader('Silahkan Login 🔒😎', divider="gray")
            
            st.info('Mohon masukan Akun yang dipakai di aplikasi app.onlinemonitoring.id', icon="ℹ️")
            username = st.text_input("Username", placeholder="Mohon Masukan username anda ...")
            password = st.text_input("Password", type="password", placeholder="Mohon Masukan password anda ...")
            
            login_button = st.form_submit_button("Login")

            if login_button:
                login(username, password)
else:
    image = Image.open('ilustrasi_old.png')
    st.markdown("""
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.6.0/css/all.min.css"/>
        <style>
            .stAppHeader{
                background: #ffe0dc;
                box-shadow: 0px 5px 10px 1px rgba(0, 0, 0, 0.2);
            }
            .stSidebar{
                border-top-right-radius: 70px;
                border-bottom-right-radius: 70px;
                box-shadow: 5px 0px 5px 1px rgba(0, 0, 0, 0.1);
                width: 270px !important;
                background: white;
            }
            .stMainMenu, .stAppDeployButton{
                display: none !important;
            } 
        </style>
    """, unsafe_allow_html=True)

    # st.title(" Data Analysis for Pasar Kerja")
    st.write("# Social Media Data Analysist 📈🚀")
    st.subheader("Dashboard for Insight & Semantic Data Analysist", divider="gray")

    # logo = st.image("logo.gif", caption="Sunrise by the mountains")
    col1, col2 = st.columns(2, gap="large")

    with col1:
        # st.image("https://cliply.co/wp-content/uploads/2019/12/371903520_SOCIAL_ICONS_TRANSPARENT_400px.gif", width=100)
        st.image(image, use_column_width=True)
        
        
        st.header('Data Filtering :')
        config = {
            'fields': {
                'UUID': {
                    'label': 'UUID',
                    'type': 'text'
                },
                'No': {
                    'label': 'No',
                    'type': 'number'
                },
                'Tanggal_Publikasi': {
                    'label': 'Tanggal Publikasi',
                    'type': 'date'
                },
                'Jam_Publikasi': {
                    'label': 'Jam Publikasi',
                    'type': 'time'
                },
                'Tanggal_Tersimpan': {
                    'label': 'Tanggal Tersimpan',
                    'type': 'datetime'
                },
                'Topik': {
                    'label': 'Topik',
                    'type': 'text'
                },
                'Akun_Judul': {
                    'label': 'Akun/Judul',
                    'type': 'text'
                },
                'Konten': {
                    'label': 'Konten',
                    'type': 'text'
                },
                'Dihapus': {
                    'label': 'Dihapus (1 Jika Ya / 0 Jika Tidak)',
                    'type': 'boolean'
                },
                'Sentimen': {
                    'label': 'Sentimen',
                    'type': 'text'
                },
                'Emotion': {
                    'label': 'Emotion',
                    'type': 'text'
                },
                'Sumber': {
                    'label': 'Sumber',
                    'type': 'text'
                },
                'Url': {
                    'label': 'Url',
                    'type': 'text'
                },
                'Followers': {
                    'label': 'Followers',
                    'type': 'number'
                },
                'Likes': {
                    'label': 'Likes',
                    'type': 'number'
                },
                'Retweets': {
                    'label': 'Retweets',
                    'type': 'number'
                },
                'ESMR': {
                    'label': 'ESMR',
                    'type': 'text'
                },
                'View': {
                    'label': 'View',
                    'type': 'number'
                },
                'Engagement': {
                    'label': 'Engagement',
                    'type': 'number'
                },
                'Location': {
                    'label': 'Location',
                    'type': 'text'
                },
                'Jenis_Akun': {
                    'label': 'Jenis Akun',
                    'type': 'select'
                },
                'Jenis_Unggahan': {
                    'label': 'Jenis Unggahan',
                    'type': 'select'
                },
                'Is_Validated': {
                    'label': 'Is Validated',
                    'type': 'bool'
                }
            }
        }

        init_tree = {
            "type": "group",
            "children": [
                {
                    "type": "rule",
                    "properties": {
                        "fieldSrc": "field",
                        "field": None,
                        "operator": None,
                        "value": [],
                        "valueSrc": []
                    }
                },
                {
                "type": "rule",
                "properties": {
                    "fieldSrc": "field",
                    "field": None,
                    "operator": None,
                    "value": [],
                    "valueSrc": []
                }
                },
                {
                "type": "rule",
                "properties": {
                    "fieldSrc": "field",
                    "field": None,
                    "operator": None,
                    "value": [],
                    "valueSrc": []
                }
                }
            ]
        }


        return_val = condition_tree(
            config,
            always_show_buttons=True,
            return_type='sql',
            placeholder='Belum ada rule filtering. Silahkan tambahkan rule baru.'
        )

    with col2:
        workbook = load_workbook('kamus_data.xlsx', read_only=True)
        st.header('Data Classification :')

        visible_sheets = [sheet for sheet in workbook.sheetnames if workbook[sheet].sheet_state == 'visible']

        keywords = st.multiselect(
            "Kamus Data Analisa Semantik :",
            visible_sheets,
            visible_sheets
        )

        
        left, right = st.columns(2)

        with right:
            popover = st.popover("Opsi Kamus Data", icon=":material/settings:", use_container_width=True)

            with open("kamus_data.xlsx", "rb") as file:
                popover.download_button(
                    label="Unduh Kamus Data",
                    data=file,
                    file_name="kamus_data.xlsx",
                    use_container_width=True,
                    icon=":material/download:",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            if popover.button("Ganti Kamus Data",use_container_width=True,icon=":material/sync:"):
                update_kamus_data()

        #connection
        # url_sheet = "https://docs.google.com/spreadsheets/d/192owtZ8J33nrYonrVKGIdcowxZJz8CObuol_ATr26w8/edit?usp=sharing"
        # conn = st.connection("gsheets", type=GSheetsConnection)
        # data = conn.query('SELECT * FROM "Tunjangan" LIMIT 10', spreadsheet=url_sheet)
        # st.dataframe(data)
        st.header('Data Cleansing :')

        left, right = st.columns(2)
        with left:
            st.toggle("Hapus Post Duplikat")
            st.toggle("Hapus karakter spesial")
            st.toggle("Hapus post bukan lowongan")

        with right:
            st.toggle("Aktifkan Bot Detector")
            st.toggle("Aktifkan Hoax Detector")
            st.toggle("Hapus Akun Bot / Hoax")
        
        st.text("")
        left, right = st.columns(2)
        with right:
           if st.button("Test Data Cleansing", icon=":material/play_circle:", use_container_width=True):
               st.write("pages/jupyter.py")

        st.header('Data Source :')

        data_source = st.radio("Data Source to Analyze :", ["***Local (From Excel)***", "***Remote (From Server)***"], horizontal=True)

        is_excel = data_source=="***Local (From Excel)***"
        
        if(is_excel):
            uploaded_file = st.file_uploader("Upload an Excel file", type=["xlsx", "xls"])
        else:
            # Base URL
            base_url = "https://test.com/new-export/456"

            # Date slider range component
            date_range = st.date_input('Start Date  - End Date :', [])
            
            if(len(date_range) > 1):
                from_date, to_date = date_range
            else:
                from_date, to_date = [datetime.now(), datetime.now()]

            
        chunksize = st.slider(
            "Total Row Per Process :",
            value=500,
            min_value=100,
            max_value=2000,
            step=100
        )

        left, middle, right = st.columns(3)
        
        if(is_excel is not True):
            with right:
                if st.button("Export Data", type="secondary", icon="📥", use_container_width=True):
                    base_url = "https://api.kurasi.media/new-export/456"

                    dynamic_url = generate_url(base_url, from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"))

                    show_dynamic_url(dynamic_url)
    
    st.text("")
    st.text("")
    st.text("")

    left, middle, middle2, right = st.columns(4) 

    with middle2:
        st.button("Simpan Konfigurasi", type="secondary", icon=":material/save:", use_container_width=True)

    with right:
        eksekusi = st.button("Proses Data Scraping", type="primary", icon="▶️", use_container_width=True)

    st.sidebar.image("https://cliply.co/wp-content/uploads/2019/12/371903520_SOCIAL_ICONS_TRANSPARENT_400px.gif", width=100)
    data_stats = get_data_stats_all_medsos()
    data_sentiment = get_data_stats_all_sentiment()

    setup_data_stats_and_sentiment(data_stats, data_sentiment)

    
    st.header('Result Proses :')

    # tab1, tab2 = st.tabs(["Insights Data Analysis", "Semantic Data Analysis"])

    if(eksekusi):
        st.toast("Memulai Proses Data Scrapping, Mohon cek Progress di section Result Proses.")
        stqdm.pandas()
        # If a file has been uploaded
        if is_excel and uploaded_file is not None:
            kd = get_kamus_data()

            # Read the Excel file in chunks
            processed_chunks = []

            with st.spinner('Memproses file excel ...'):
                total_rows = pd.read_excel(uploaded_file, engine='openpyxl', sheet_name="Media Sosial").shape[0]
                num_chunks = (total_rows // chunksize) + (total_rows % chunksize > 0)


            progress_bar = st.progress(0, f"Memproses {num_chunks} Chunk Data.")

            for i in range(num_chunks):
                # Update the progress bar
                progress_bar.progress((i + 1) / num_chunks, f"Memproses {i + 1}/{num_chunks} chunk data")

                # Use skiprows to read specific chunks
                chunk = pd.read_excel(
                    uploaded_file,
                    sheet_name="Media Sosial",
                    engine='openpyxl',
                    skiprows=range(1, i * chunksize + 1),  # Skip rows that have already been processed
                    nrows=chunksize,  # Read only 'chunksize' rows at a time
                )
                
                # Process the current chunk
                processed_chunk = process_chunk(chunk, kd)
                processed_chunks.append(processed_chunk)
                
                
                progress_bar.progress((i + 1) / num_chunks, f"Istirahat 2 detik ...")
                time.sleep(2)  # Pause for 2 seconds between chunks
            
            # Combine all processed chunks into a single DataFrame
            final_df = pd.concat(processed_chunks, ignore_index=True)
            
            progress_bar.empty()
            eksekusi_excel2(final_df, kd)
            
            # eksekusi_excel(tab1, tab2, df)
        elif is_excel is not True and uploaded_file is None:
            base_url = "https://api.kurasi.media/new-export/456"
            dynamic_url = generate_url(base_url, from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"))

            try:
                with st.spinner('Mengambil data ke Server, mohon tunggu sebentar ...'):
                    response = requests.get(dynamic_url)
                    response.raise_for_status()

                    file_bytes = BytesIO(response.content)
                    df = pd.read_excel(file_bytes, sheet_name="Media Sosial")

                # eksekusi_excel(tab1, tab2, df)

            except requests.exceptions.RequestException as e:
                st.error(f"Error: {e}")
        else:
            st.toast("Ooppss, There's Something Wrong!", icon="ℹ️")
    else:
        st.info("Data will display here, please execute proses data first!", icon="ℹ️")
        