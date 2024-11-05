import streamlit as st
import pandas as pd
import plotly.express as px
from PIL import Image
import re
import requests
from openpyxl import load_workbook
# from stqdm import stqdm
from io import BytesIO
from datetime import datetime
from streamlit_condition_tree import condition_tree, config_from_dataframe
import time
import pymongo
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import page_service as ps
from streamlit_tags import st_tags
from barfi import st_barfi, Block, barfi_schemas
from barfi.manage_schema import load_schema_name

# init page
st.set_page_config(
    page_title="Social Media Data Analysist",
    page_icon="📊",
    layout="wide"
)

ps.setup_style_awal(st)

img_logo = Image.open('logo_pasker.png')
st.logo(img_logo, size="large")

with open('config.yaml') as file:
    config_yaml = yaml.load(file, Loader=SafeLoader)

# password = stauth.Hasher.hash_passwords(config_yaml['credentials'])
# st.write(password)

authenticator = stauth.Authenticate(
    config_yaml['credentials'],
    config_yaml['cookie']['name'],
    config_yaml['cookie']['key'],
    config_yaml['cookie']['expiry_days'],
    auto_hash=False
)

authenticator.login(location="unrendered")

st.session_state["auth_obj"] = authenticator

client = None
db = None

@st.cache_resource
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongo"])

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

def classify_akun(nama_akun, is_pers_value):
    kat_akun = "Akun Netizen"
    pattern = re.compile(r'|'.join(kat_akun_loker), re.IGNORECASE)

    if(is_pers_value):
        return "Akun Pers"
    
    if pattern.search(nama_akun):
        return "Akun Loker"
    
    return kat_akun

def kalkulasi_persentase_lowongan(params):
    result = 0
    params.insert(0, params.pop())

    for i, param in enumerate(params):
        if(i==0 and param=="Akun Loker"):
            result += nilai_bobot[0]
        elif(i != 0 and "tdk_ada_informasi" not in param):
            result += nilai_bobot[i]

    return result

def rand_persen_wait(persen):
    result = False
    
    if(persen > 5 and persen < 9):
        result = True
    elif(persen > 10 and persen < 15):
        result = True
    elif(persen > 20 and persen < 25):
        result = True
    elif(persen > 30 and persen < 35):
        result = True
    elif(persen > 40 and persen < 45):
        result = True
    elif(persen > 50 and persen < 55):
        result = True
    elif(persen > 60 and persen < 65):
        result = True
    elif(persen > 70 and persen < 75):
        result = True
    elif(persen > 80 and persen < 85):
        result = True
    elif(persen > 90 and persen < 95):
        result = True

    return result

def process_chunk(chunk, kd, collection_name, progress_bar):
    for index, row in chunk.iterrows():
        # Get the 'Konten' value once for this row
        konten_value = str(row['Konten'])
        akun_value = str(row['Akun/Judul'])
        is_pers_value = str(row['Jenis Akun']) == "Pers"
        data_params_kalkulasi_persentase_lowongan = []
        
        # Calculate and set classifications for each item in kd
        for item in kd:
            # Assuming item["kamus_data"] is the name of the new column
            column_name = item["kamus_data"]

            if column_name not in chunk.columns:
                # Create the column if it doesn't exist
                chunk[column_name] = None
            
            # Apply classify_job_category2 and assign result to the specific cell
            result_1 = classify_job_category2(konten_value, item)
            chunk.at[index, column_name] = result_1
            data_params_kalkulasi_persentase_lowongan.append(result_1)


        # Mengkliasifikasikan Tipe Akun
        if "Klasifikasi Akun" not in chunk.columns:
            chunk["Klasifikasi Akun"] = None

        result_2 = classify_akun(akun_value, is_pers_value)
        chunk.at[index, "Klasifikasi Akun"] = result_2
        data_params_kalkulasi_persentase_lowongan.append(result_2)


        # Menghitung Persentase Lowongan
        if "Persentase Lowongan" not in chunk.columns:
            chunk["Persentase Lowongan"] = None

        chunk.at[index, "Persentase Lowongan"] = kalkulasi_persentase_lowongan(data_params_kalkulasi_persentase_lowongan)


        progress_bar.progress(index/len(chunk), f"Menganalisa data baris ke-{index} dari {len(chunk)} data.")

    progress_bar.empty()

    collection = db[collection_name]
    data_dict = chunk.to_dict(orient="records")
    collection.insert_many(data_dict)

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
kat_akun_loker = None
nilai_bobot = []

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


def convert_format_flow(data, current_config):
    # Map names to options from current_config
    options_map = {node["name"]: node["options"] for node in current_config["nodes"]}

    # Ensure the Data Source node and validate it
    data_source_nodes = [node for node, details in data.items() if details.get("type") == "Data Source"]

    if len(data_source_nodes) != 1:
        raise ValueError("There must be exactly one node with type 'Data Source'.")

    data_source_node = data_source_nodes[0]

    # Check if the Data Source node has no incoming connections (it's the root)
    incoming_connections = {node: [] for node in data.keys()}

    # Populate incoming connections dictionary
    for node, details in data.items():
        interfaces = details.get("interfaces", {})
        for conn in interfaces.values():
            if conn.get("type") == "output":
                for target_node in conn.get("to", {}).keys():
                    incoming_connections[target_node].append(node)

    if incoming_connections[data_source_node]:
        raise ValueError("The 'Data Source' node must have no incoming connections and be the root node.")

    # Build adjacency list for topological sorting
    graph = {}
    in_degree = {}

    # Initialize graph and in-degree dictionary
    for node, details in data.items():
        graph[node] = []
        in_degree[node] = 0

    # Populate graph and in-degree based on connections
    for node, details in data.items():
        interfaces = details.get("interfaces", {})
        for interface, conn in interfaces.items():
            if conn.get("type") == "output":
                for target_node, target_interface in conn.get("to", {}).items():
                    graph[node].append(target_node)
                    in_degree[target_node] += 1

    # Topological sort using Kahn's Algorithm, starting with Data Source
    sorted_nodes = []
    zero_in_degree = [data_source_node]  # Start with the Data Source node

    while zero_in_degree:
        current = zero_in_degree.pop(0)
        sorted_nodes.append(current)
        
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                zero_in_degree.append(neighbor)

    # Reorder the original data based on sorted_nodes and add options
    sorted_data_with_options = {}
    for node in sorted_nodes:
        sorted_data_with_options[node] = data[node]
        sorted_data_with_options[node]["options"] = options_map.get(node, [])  # Add options if available

    parsed_data = []
    for node_name, node_data in sorted_data_with_options.items():
        # Extract and format options
        options = [{"label": label.lower(), "value": value} for label, value in node_data.get("options", [])]
        # Append the formatted node data
        parsed_data.append({
            "node_name": node_name,
            "node_type": node_data["type"],
            "options": options
        })
    # Convert sorted data to JSON
    # sorted_json_with_options = json.dumps(sorted_data_with_options, indent=2)
    return parsed_data


@st.dialog("Konfigurasi Mesin Klasifikasi", width="large")
def visual_builder():
    source = Block(name='Data Source')
    source.add_output("data")
    source.add_option("test", type='input')
    source.add_option("Tipe", type='select', items=["Text", "Excel"])

    operator = Block(name='Operator')
    operator.add_input()
    operator.add_option("Tipe", type='select', items=["Text", "Excel"])
    operator.add_output()


    result = Block(name='Result')
    result.add_input()

    # load_schema = st.selectbox('Select a saved schema:', barfi_schemas())

    barfi_result = st_barfi(base_blocks=[source, operator, result], load_schema="latest", compute_engine=True, key="barfi_elem")
    
    if barfi_result:
        result = convert_format_flow(barfi_result, load_schema_name("latest"))

        st.write(result)

def convert_df_to_excel(df):
    # Create a copy of the DataFrame to avoid modifying the original
    df_copy = df.copy()
    
    # Convert any column with list values to comma-separated strings
    for col in df_copy.columns:
        df_copy[col] = df_copy[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
    
    # Write the DataFrame to an Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_copy.to_excel(writer, index=False, sheet_name='Sheet1')
    output.seek(0)
    return output

def eksekusi_excel2(df, kd):

    # for item in stqdm(kd, desc="Mengalisa data semantik konten ...", backend=False, frontend=True):
        # df[item["kamus_data"]] = df['Konten'].progress_apply(lambda x: classify_job_category2(str(x), item))

    formatted_array = [item["kamus_data"] for item in kd]
    st.dataframe(df[['Konten', 'Url'] + formatted_array])

    
    col1, col2, col3, col4 = st.columns(4, gap="large")

    with col4:
        st.download_button(
            label="Download Excel file",
            use_container_width=True,
            type="primary",
            icon=":material/download:",
            data=convert_df_to_excel(df[['Konten', 'Url'] + formatted_array]),
            file_name='hasil_proses.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    return
    
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


def node_data_source(cfg):
    st.write("ini node " + cfg["node_name"])

def node_operator(cfg):
    st.write("ini node " + cfg["node_name"])

def node_result(cfg):
    st.write("ini node " + cfg["node_name"])

def init_config_alur_klasifikasi(list_node):
    st.write(list_node)
    for idx, node in enumerate(list_node):
        with st.expander(f"Konfigurasi Tahap #{idx + 1}", expanded=True):
            st.write(f"##### {node['node_name']}")
            match node["node_type"]:
                case "Data Source":
                    node_data_source(node)
                case "Operator":
                    node_operator(node)
                case "Result":
                    node_result(node)
                case default:
                    return None

if not st.session_state.authentication_status:
    st.write("# Social Media Data Analysist 📈🚀")
    st.subheader("Dashboard for Insight & Semantic Data Analysist")

    col1, col2 = st.columns([2.5,1], vertical_alignment="center", gap="large")
    with col1:
        image_login = Image.open('login_analis.jpg')
        st.image(image_login, use_column_width=True)
    with col2:
        try: 
            authenticator.login(fields=dict({'Form name':'Silahkan Login 🔒😎', 'Username':'Masukan Username :', 'Password':'Masukan Password :', 'Login':'Lanjukan', 'Captcha':'Masukan Kode di bawah :'}), captcha=True)
        except Exception as e:
            st.toast(e)
else:
    st.markdown("""
        <style>
            div[data-testid='stNumberInputContainer']{
                border: 5px solid #f0f2f6;
            }
                
            div[data-testid='stNumberInputContainer'] input{
                background: white;
            }
                
            .stMainBlockContainer{
                padding-left: 40px;
                padding-right: 40px;
                padding-top: 35px;
                padding-bottom: 45px;
            }
                
            .stMainBlockContainer.block-container > div > div > div > div > div.stColumn:nth-child(1) > div{
                top: 70px;
                position: sticky;
            }
        </style>
    """, unsafe_allow_html=True)

    ps.setup_st_sidebar(st, authenticator)

    client = init_connection()
    db = client.medmon

    items = db.config.find()
    arr = list(items)
    config = arr[0] if len(arr) > 0 else None

    # image = Image.open('ilustrasi_new.gif')
    

    # logo = st.image("logo.gif", caption="Sunrise by the mountains")
    col1, col2 = st.columns([1.4,2], gap="small")

    with col1:
        # st.write("##### Identifikasi dan Klasifikasi Data Text tidak ter-struktur dengan akurat & presisi.")

        with st.container(key="sticky"):
            st.write("## Mesin Klasifikasi Text 🚀")
            st.write("###### Klasifikasi Data Text tidak ter-struktur dengan akurat & presisi.")
            # st.divider()
            # st.write("## Mesin Klasifikasi Text 📈🚀")
            st.markdown(f"""
                <style>
                    .st-key-vb_button {{
                        position: absolute;
                        bottom: 10px;
                        text-align: right;
                        right: 20px;
                    }}
                </style>
            """, unsafe_allow_html=True)

            st.html(
                """
                    <img src='https://i.ibb.co.com/mHJpjNb/mantap.gif' style='width: 100%; border-radius: 0.5rem;'/>
                """
            )

            # if st.button("", type="primary", icon=":material/tune:", key="vb_button"):
                # visual_builder()
        
        st.progress(0, "Progress Chunk & Split Data ...")
        st.progress(0, "Progress Indentifikasi & Klasifikasi Text ...")

        left, right = st.columns(2)
        
        with left:
            st.button("Simpan Konfigurasi", type="secondary", icon=":material/save:", use_container_width=True)
        
        with right:
            st.button("Mulai Klasifikasi", icon=":material/play_arrow:", use_container_width=True, type="primary")
        
    with col2:
        with st.container(border=True):
            st.write("#### Alur Proses Klasifikasi :")

            tab1, tab2, tab3, tab4 = st.tabs(["Visual Flow Builder", "Hasil Proses Klasifikasi", "Riwayat Pemrosesan",  "Guideline Penggunaan"])
            
            with tab1:
                source = Block(name='Data Source')
                source.add_output("data")
                source.add_option("test", type='input')
                source.add_option("Tipe", type='select', items=["Text", "Excel"])

                operator = Block(name='Operator')
                operator.add_input()
                operator.add_option("Tipe", type='select', items=["Text", "Excel"])
                operator.add_output()


                result = Block(name='Result')
                result.add_input()

                # load_schema = st.selectbox('Select a saved schema:', barfi_schemas())

                barfi_result = st_barfi(base_blocks=[source, operator, result], load_schema="latest", compute_engine=True, key="barfi_elem")

                if barfi_result:
                    result = convert_format_flow(barfi_result, load_schema_name("latest"))

                    init_config_alur_klasifikasi(result)

            with tab2:
                with st.container(height=1800, border=False):
                    st.write("Under Development")

            with tab3:
                with st.container(height=1800, border=False):
                    st.write("Under Development")
            with tab4:
                st.write("## FAQ & Guideline")
                with st.expander("Bagaimana cara meyimpan dan meng-update Alur pada Visual Builder ?", icon=":material/help:"):
                    st.markdown("""
                        ### Panduan Penyimpanan dan Pembaruan Alur Data

                        Berikut ini adalah panduan untuk melakukan penyimpanan data dan memperbarui alur dengan benar. Pastikan untuk mengikuti setiap langkah dengan saksama.

                        ---

                        #### 1. Menyimpan Data (Save Data)

                        Ketika Anda melakukan perubahan pada panel, sangat penting untuk menyimpan data agar perubahan tersebut tidak hilang. Berikut adalah langkah-langkah untuk menyimpan data:

                        ### Langkah-langkah:

                        1. **Pastikan Anda berada di dalam panel yang sesuai** – Pastikan semua perubahan yang Anda inginkan sudah dilakukan pada panel.
                        
                        2. **Tekan Menu** – Cari dan tekan opsi "Menu" pada antarmuka aplikasi Anda. Menu ini biasanya terletak di bagian atas atau samping aplikasi, tergantung pada desain antarmuka yang digunakan.

                        3. **Pilih Opsi Save** – Setelah masuk ke menu, pilih opsi "Save" untuk menyimpan data terbaru.

                        4. **Masukkan Nama Schema** – Pada kolom **Nama Schema**, ketikkan `'latest'`. Ini akan menjadi penamaan schema data terbaru yang telah Anda simpan.

                        5. **Tekan Tombol Konfirmasi** – Tekan tombol konfirmasi (biasanya bertuliskan "Save" atau "Simpan") untuk menyimpan data dengan nama schema `'latest'`.

                        ---

                        #### 2. Memperbarui Alur (Execute)

                        Setelah data tersimpan, jika Anda ingin memperbarui alur berdasarkan data terbaru, lakukan langkah berikut:

                        ##### Langkah-langkah:

                        1. **Periksa Kembali Data yang Disimpan** – Pastikan bahwa data terbaru telah disimpan dengan benar, dan nama schema yang digunakan adalah `'latest'`.

                        2. **Tekan Tombol Execute** – Cari dan tekan tombol `Execute` untuk menjalankan pembaruan alur. Tombol ini biasanya terdapat di panel kontrol utama atau di bagian bawah setelah menyimpan perubahan.

                        3. **Konfirmasi Pembaruan** – Tunggu beberapa saat hingga proses eksekusi selesai. Alur akan diperbarui dengan data yang terbaru sesuai dengan perubahan yang telah disimpan.

                        ---

                        ##### Catatan Tambahan:
                        - **Periksa Notifikasi** – Setelah melakukan `Execute`, periksa notifikasi atau pesan di layar untuk memastikan bahwa alur telah diperbarui dengan sukses.
                        - **Ulangi Langkah Jika Perlu** – Jika alur tidak diperbarui seperti yang diharapkan, ulangi langkah-langkah di atas dan pastikan semua perubahan sudah disimpan dengan nama schema yang benar.
                    """)
                
                with st.expander("Bagaimana Memulai Proses Klasifikasi ?", icon=":material/help:"):
                    st.write("test")
                st.info("Dengan mengikuti panduan ini, Anda dapat memastikan bahwa data yang Anda perbarui tersimpan dan alur diperbarui dengan baik. Pastikan untuk selalu menyimpan data sebelum menjalankan `Execute` agar tidak kehilangan perubahan yang telah dilakukan.", icon=":material/info:")