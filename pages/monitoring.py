import streamlit as st
from PIL import Image
import page_service as ps
from pymongo import MongoClient, ASCENDING, DESCENDING
# from datetime import datetime, timedelta
import datetime
import pandas as pd
import plotly.express as px
from io import BytesIO

import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import pydeck as pdk

from pptx import Presentation
from pptx.util import Inches
import tempfile

import requests
import base64

from pptx.dml.color import RGBColor

coordinates_data = {
    "KALIMANTAN BARAT": (-0.0263, 109.3425),
    "PAPUA SELATAN": (-5.4656, 140.6324),
    "DAERAH ISTIMEWA YOGYAKARTA": (-7.7972, 110.3688),
    "SULAWESI BARAT": (-2.6713, 119.4543),
    "BALI": (-8.4095, 115.1889),
    "ACEH": (4.6951, 96.7494),
    "SULAWESI TENGAH": (-1.4307, 121.4456),
    "PAPUA BARAT": (-1.3361, 133.1747),
    "KALIMANTAN TIMUR": (0.5387, 116.4194),
    "SULAWESI SELATAN": (-4.6796, 119.7320),
    "SULAWESI UTARA": (1.4379, 124.8489),
    "MALUKU UTARA": (1.6944, 127.8080),
    "KALIMANTAN UTARA": (2.1381, 117.4165),
    "JAWA TENGAH": (-7.1500, 110.1403),
    "PAPUA": (-4.2699, 138.0804),
    "SUMATERA SELATAN": (-3.3194, 104.9147),
    "GORONTALO": (0.6788, 122.4559),
    "PAPUA TENGAH": (-3.8825, 137.1627),
    "RIAU": (0.5104, 101.4381),
    "KALIMANTAN TENGAH": (-1.6815, 113.3824),
    "KEPULAUAN RIAU": (3.9457, 108.1429),
    "KALIMANTAN SELATAN": (-2.9741, 115.5557),
    "BANTEN": (-6.1202, 106.1503),
    "JAMBI": (-1.6108, 103.6119),
    "LAMPUNG": (-5.4500, 105.2663),
    "BENGKULU": (-3.8004, 102.2655),
    "SUMATERA BARAT": (-0.7371, 100.2711),
    "JAWA BARAT": (-6.9147, 107.6098),
    "NUSA TENGGARA BARAT": (-8.6529, 116.3249),
    "NUSA TENGGARA TIMUR": (-9.4750, 119.8707),
    "PAPUA PEGUNUNGAN": (-3.7330, 138.0883),
    "JAWA TIMUR": (-7.2504, 112.7688),
    "KEPULAUAN BANGKA BELITUNG": (-2.7455, 106.1134),
    "SULAWESI TENGGARA": (-4.1470, 122.1746),
    "SUMATERA UTARA": (2.2583, 99.6115),
    "MALUKU": (-3.2384, 129.4921),
    "DKI JAKARTA": (-6.2088, 106.8456)
}

# Inisiasi page
st.set_page_config(
    page_title="Analisa Data Pasker",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="auto"
)


color_sequence = px.colors.qualitative.D3

with open('config.yaml') as file:
    config_yaml = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config_yaml['credentials'],
    config_yaml['cookie']['name'],
    config_yaml['cookie']['key'],
    config_yaml['cookie']['expiry_days'],
    auto_hash=False
)

authenticator.login(location="unrendered")

if not st.session_state.authentication_status:
    st.switch_page("streamlit_app.py")

st.markdown("""
    <style>
        .stMainBlockContainer{
            padding-left: 40px;
            padding-right: 40px;
            padding-top: 20px;
            padding-bottom: 40px;
        }
    </style>
""", unsafe_allow_html=True)
ps.setup_style_awal(st)

img_logo = Image.open('logo_pasker.png')
st.logo(img_logo, size="large")


if(authenticator):
    ps.setup_st_sidebar(st, authenticator)

#init DB
@st.cache_resource
def init_connection():
    return MongoClient(**st.secrets["mongo"])

client = init_connection()
db = client.medmon
collection = db["hasil_proses_v1"]

# Main Page Function
# st.write("# Analisa Data 🕵️‍♂️🚀")
# st.subheader("Semua Data Hasil Scraping & Klasifikasi")


def fetch_data(search_query, sort_by, sort_dir):
    query = {}

    if search_query:
        query = {"$text": {"$search": search_query}}

    page = st.session_state["page"] if "page" in st.session_state else 0
    page_size = st.session_state["page_size"] if "page_size" in st.session_state else 10
    
    sort_direction = ASCENDING if sort_dir == "🔼" else DESCENDING
    
    cursor = collection.find(query).sort(sort_by, sort_direction).skip(page * page_size).limit(page_size)
    data = list(cursor)
    
    df = pd.DataFrame(data)

    if '_id' in df.columns:
        df = df.drop(columns=['_id'])
    

    start_index = page * page_size
    df.index = range(start_index, start_index + len(df))

    return df

def fetch_data(search_query, sort_by, sort_dir, filters, page, page_size):
    # Initialize query
    query = {}
    page = page - 1

    # Apply text search if provided
    if search_query:
        query["$text"] = {"$search": search_query}
    
    if "tanggal" in filters:
        start_date, end_date = [
            date if isinstance(date, str) else date.strftime("%Y-%m-%d")
            for date in filters["tanggal"]
        ]
        query["Tanggal Publikasi"] = {"$gte": start_date, "$lte": end_date}

    # Apply filter for "Klasifikasi Akun"
    if "jenis_akun" in filters:
        query["Klasifikasi Akun"] = {"$in": filters["jenis_akun"]}
    
    # Apply filter for "Sumber"
    if "sumber" in filters:
        query["Sumber"] = {"$in": filters["sumber"]}

    # Pagination settings
    # page = st.session_state.get("page", 0)
    # page_size = st.session_state.get("page_size", 10)
    
    # Sorting direction
    sort_direction = ASCENDING if sort_dir == "🔼" else DESCENDING

    # Fetch filtered and sorted data from MongoDB
    cursor = (
        collection.find(query)
        .sort(sort_by, sort_direction)
        .skip(page * page_size)
        .limit(page_size)
    )
    data = list(cursor)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Remove MongoDB ID column
    if '_id' in df.columns:
        df = df.drop(columns=['_id'])

    # Apply column selection filter
    if "list_kolom" in filters:
        available_columns = [col for col in filters["list_kolom"] if col in df.columns]
        df = df[available_columns]

    # Update DataFrame index to match pagination
    start_index = page * page_size
    df.index = range(start_index, start_index + len(df))

    return df

def get_coordinates(province_name):
    if province_name in coordinates_data:
        return coordinates_data[province_name]
    else:
        return None, None
    
kd = ["Tipe Pekerjaan", "Tingkat Pekerjaan", "Tingkat Pendidikan", "Pengalaman Kerja", 
      "Tunjangan", "Jenis Kelamin", "Cara Kerja", "Lokasi", "Lokasi Kota", 
      "Keterampilan Bahasa", "Keterampilan Teknis", "Keterampilan Non Teknis", "Jabatan"]

# Function to save figure and add to PowerPoint
def save_chart_to_slide(presentation, fig, title, df=None):
    slide_layout = presentation.slide_layouts[5]  # Title slide layout
    slide = presentation.slides.add_slide(slide_layout)
    title_placeholder = slide.shapes.title
    title_placeholder.text = title

    # Save Plotly figure as an image
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmpfile:
        fig.write_image(tmpfile.name, format="png", scale=2, width=1200, height=800)
        slide.shapes.add_picture(tmpfile.name, Inches(0), Inches(1.5), width=Inches(7.5))
    
    if df is not None and not df.empty:
        add_table_from_df_to_slide(presentation=presentation, df=df, title=None, slide=slide)

def add_table_from_df_to_slide(presentation, df, title=None, slide=None):
    if not slide:
        # Add a slide with the specified layout
        slide = presentation.slides.add_slide(presentation.slide_layouts[5])
        x, y, cx, cy = Inches(1.2), Inches(1.8), Inches(7.5), Inches(4)
    else:
        x, y, cx, cy = Inches(6.5), Inches(3), Inches(3.1), Inches(3.7)

    if title:
        title_placeholder = slide.shapes.title
        title_placeholder.text = title

    # Define the number of rows and columns for the table
    rows, cols = df.shape[0] + 1, df.shape[1]
    
    # Add table placeholder (position and size: x, y, width, height)
    table = slide.shapes.add_table(rows, cols, x, y, cx, cy).table
    
    # Add header row
    for col_idx, col_name in enumerate(df.columns):
        table.cell(0, col_idx).text = col_name
    
    # Add data to table
    for row_idx, (index, row) in enumerate(df.iterrows(), start=1):
        for col_idx, value in enumerate(row):
            table.cell(row_idx, col_idx).text = str(value)

    
    

# Create PowerPoint Presentation
presentation = Presentation()

slide_layout = presentation.slide_layouts[0]
slide = presentation.slides.add_slide(slide_layout)

background = slide.background
fill = background.fill
fill.solid()
fill.fore_color.rgb = RGBColor(173, 216, 230)  # Light blue color

# Add the company name
title = slide.shapes.title
title.text = "Weekly Report Pasker"

# Add the presentation title
subtitle = slide.placeholders[1]  # Subtitle placeholder (varies by layout)

@st.cache_data()
def get_all_column():
    # result = collection.find_one().keys()
    return None

@st.cache_data(ttl=180)
def get_top_accounts(alur_waktu):
    pipeline = [
        {"$match": {
            "Klasifikasi Akun": "Akun Loker",
            "Tanggal Publikasi": {"$gte": alur_waktu[0], "$lte": alur_waktu[1]}
        }},  # Filter by category
        {
            "$group": {
                "_id": "$Akun/Judul",
                "Followers": {"$last": "$Followers"},
                "Sumber": {"$last": "$Sumber"}  # Include Sumber in the output
            }
        },
        {"$sort": {"Followers": -1}},
        {"$limit": 10},
        {
            "$project": {
                "_id": 1,
                "Sumber": 1,
                "Followers": 1,
            }
        }
    ]
    return pd.DataFrame(list(collection.aggregate(pipeline)))

@st.cache_data(ttl=180)
def get_total_posts_by_classification(alur_waktu):
    pipeline = [
        {"$match": { "Tanggal Publikasi": {"$gte": alur_waktu[0], "$lte": alur_waktu[1]}}},
        {"$group": {"_id": "$Klasifikasi Akun", "Count": {"$sum": 1}}}
    ]
    return pd.DataFrame(list(collection.aggregate(pipeline)))

@st.cache_data(ttl=180)
def get_category_counts(category_field,alur_waktu):
    # If the category is "Jenis Kelamin", handle it specifically
    match_stage = {"$match": {}}
    match_stage["$match"]["Tanggal Publikasi"] = {"$gte": alur_waktu[0], "$lte": alur_waktu[1]}

    if category_field == "Jenis Kelamin":
        pipeline = [
            match_stage,
            {
                "$project": {
                    "Jenis Kelamin": {
                        "$cond": [
                            {
                                "$eq": [
                                    {"$size": {"$setIntersection": [{"$ifNull": ["$Jenis Kelamin", []]}, ["Laki-Laki", "Perempuan"]]}},
                                    2
                                ]
                            },
                            "Laki-laki & Perempuan",  # Both genders are present
                            {"$arrayElemAt": ["$Jenis Kelamin", 0]}  # Just one gender
                        ]
                    }
                }
            },
            {"$group": {"_id": "$Jenis Kelamin", "Count": {"$sum": 1}}},
            {"$match": {"_id": {"$ne": "tdk_ada_informasi"}}}
        ]
    else:
        # Default pipeline for other categories
        pipeline = [
            match_stage,
            {"$unwind": f"${category_field}"},
            {"$group": {"_id": f"${category_field}", "Count": {"$sum": 1}}},
            {"$match": {"_id": {"$ne": "tdk_ada_informasi"}}}
        ]
    
    # Execute the aggregation pipeline and return as DataFrame
    return pd.DataFrame(list(collection.aggregate(pipeline)))

@st.dialog("Download disini :")
def save_ppt():
    with st.spinner("Menyiapkan File PPT ke Google Drive ..."):
        data_presentation = st.session_state["presentation"]
        
        with tempfile.NamedTemporaryFile(suffix=".pptx", delete=False) as tmpfile:
            data_presentation.save(tmpfile.name)

        # Read the file and encode in Base64
            with open(tmpfile.name, "rb") as file:
                encoded_file = base64.b64encode(file.read()).decode("utf-8")

            # Prepare data payload
            data = {
                "folderId": "1bMhZ6lnZQLO2hR2w73GQ1cEvD5VNsOnu",
                "fileData": encoded_file,
                "fileName": "dashboard_weekly_presentation_pasker.pptx"
            }

            # Endpoint URL
            endpoint = "https://script.google.com/macros/s/AKfycbypnLPxFKWlAFWcnx6U7OR28rsADxWBe5r2Np2UTjKMmQpQLYmzY0YiZw2lUDtTGh7_/exec?rute=upload_ppt"
            
            # Send POST request
            response = requests.post(endpoint, json=data)
    
    data_resp = response.json()
    url = data_resp.get('url', 'No URL returned')

    # st.write(data_resp)
    st.success("File Siap di Download", icon=":material/check_circle:")
    st.link_button("Lihat PPT", url, icon=":material/description:", type="primary", use_container_width=True)

filter_option = {
    "list_kolom": None,
    "tanggal": None,
    "jenis_akun": None,
    "sumber": None
}
with st.container(border=True):
    col = st.columns([4.5,1], vertical_alignment="center")

    with col[0]:
        st.write("## Analisa Data 🕵️‍♂️🚀")
        st.write("###### Semua Data Hasil Scraping & Klasifikasi")
    with col[1]:
        popover = st.popover("Export Data", icon=":material/download:", use_container_width=True)

        if popover.button("Dashboard (PPT)", icon=":material/animated_images:"):
            save_ppt()

        popover.button("Scraping (XLSX)", icon=":material/table:")

    main_tabs = st.tabs(["Dashboard", "Scraping Data"])
    
    with main_tabs[0]:
        with st.container(border=True):
            st.write("##### 📆 Timeline Data : ")
            alur_waktu = st.slider("", label_visibility="collapsed", value=[datetime.datetime(2024, 7, 1, 0, 0), datetime.datetime(2024, 11, 1, 0, 0)], min_value=datetime.datetime(2024, 7, 1), max_value=datetime.datetime(2024, 12, 31, 1, 1), format="MM/DD/YY")
        
        aw = [
            date if isinstance(date, str) else date.strftime("%Y-%m-%d")
            for date in alur_waktu
        ]

        
        subtitle.text = str(aw)
        
        with st.container(border=True):
            col = st.columns([5,1.2], vertical_alignment="center")
            
            with col[0]:
                st.write("##### Distribusi Jumlah Lowongan Berdasarkan :red[Lokasi Provinsi]")
            with col[1]:
                format_map = st.radio("", ["Map 2D", "Map 3D"], horizontal=True, label_visibility="collapsed")

            df_lokasi = get_category_counts("Lokasi",aw)
            df_lokasi.columns = ["Lokasi", 'Count']

            # Add coordinates to the DataFrame
            df_lokasi[['lat', 'lon']] = df_lokasi['Lokasi'].apply(lambda x: pd.Series(get_coordinates(x)))

            # Filter out any rows with missing coordinates
            df_lokasi = df_lokasi.dropna(subset=['lat', 'lon'])
            df_lokasi['Size'] = df_lokasi['Count'] * 5  # Scale the size by multiplying by 100
            df_lokasi['Elevation'] = df_lokasi['Count'] * 5  # Scale the elevation by multiplying by 100
            

            if format_map=="Map 2D":
                st.map(
                    df_lokasi,
                    latitude="lat",
                    longitude="lon",
                    size="Size",  # Use the 'Count' column to scale the dot size
                    zoom=4
                )
            else:
                layer = pdk.Layer(
                    "ColumnLayer",  # Use ColumnLayer for 3D columns
                    data=df_lokasi,
                    get_position='[lon, lat]',
                    get_elevation='Elevation',  # Use 'Elevation' column for 3D height
                    elevation_scale=10,  # Adjust this scale to modify column height
                    radius=50000,  # Adjust radius for column size
                    get_fill_color='[255, 0, 0, 150]',  # RGBA color for columns
                    pickable=True,
                    auto_highlight=True
                )

                # Define the view state for the map
                view_state = pdk.ViewState(
                    latitude=df_lokasi['lat'].mean(),
                    longitude=df_lokasi['lon'].mean(),
                    zoom=4,
                    pitch=50,  # Angle for 3D effect
                    bearing=0
                )

                # Add tooltip configuration
                tooltip = {
                    "html": "<b>Province:</b> {Lokasi} <br/> <b>Job Count:</b> {Count}",
                    "style": {"color": "white"}
                }

                # Create the PyDeck chart
                st.pydeck_chart(pdk.Deck(
                    map_style="mapbox://styles/mapbox/light-v9",
                    initial_view_state=view_state,
                    layers=[layer],
                    tooltip=tooltip
                ))
        top_chart = st.columns(3, gap="small")
        # Top 10 Accounts with Most Followers

        with top_chart[0]:
            with st.container(border=True):
                with st.spinner('Preparing data.'):
                    st.write("##### Top 10 :red[Akun Loker Follower] Terbanyak")
                    follower_by_account = get_top_accounts(aw)
                    st.dataframe(follower_by_account, hide_index=True, use_container_width=True)
                    add_table_from_df_to_slide(presentation=presentation, slide=None, df=follower_by_account, title="Top 10 Akun Loker Follower Terbanyak")

        with top_chart[1]:
            with st.container(border=True):
                with st.spinner('Preparing data.'):
                    st.write("##### Distribusi Data Berdasarkan :red[Sumber Sosial Media]")
                    
                    # Query to count each source
                    source_count = collection.aggregate([
                        {"$match": {
                            "Tanggal Publikasi": {"$gte": aw[0], "$lte": aw[1]}
                        }},
                        {"$group": {"_id": "$Sumber", "Count": {"$sum": 1}}}
                    ])
                    source_count_df = pd.DataFrame(list(source_count))
                    source_count_df.columns = ['Sumber', 'Count']

                    # Pie chart for source distribution
                    fig_source_pie = px.pie(source_count_df, names='Sumber', values='Count', color_discrete_sequence=color_sequence)
                    st.plotly_chart(fig_source_pie)
                    save_chart_to_slide(presentation, fig_source_pie, "Distribusi Sumber Sosial Media", source_count_df)

        # Total Posts by Account Classification
        with top_chart[2]:
            with st.container(border=True):
                with st.spinner('Preparing data.'):
                    st.write("##### Total Postingan Berdasarkan :red[Klasifikasi Akun]")
                    sentimen_count = get_total_posts_by_classification(aw)
                    sentimen_count.columns = ['Klasifikasi Akun', 'Count']
                    fig_sentimen = px.bar(sentimen_count, x='Klasifikasi Akun', y='Count', color_discrete_sequence=color_sequence)
                    st.plotly_chart(fig_sentimen)
                    save_chart_to_slide(presentation, fig_sentimen, "Total Postingan Berdasarkan Klasifikasi Akun", sentimen_count)
        # Loop through categories and generate charts
        for idx, item in enumerate(kd):
            with st.container(border=True):
                with st.spinner('Preparing data.'):
                    st.write(f'##### Distribusi Data :red[{item}] di Setiap Postingan')

                    # Get category count data
                    category_count = get_category_counts(item,aw)
                    category_count.columns = [item, 'Count']

                    col1, col2 = st.columns(2, gap="small")

                    if idx % 3 == 0:
                        with col1:
                            st.dataframe(category_count, hide_index=True)
                        with col2:
                            fig_pie_category = px.pie(category_count, names=item, values='Count', color_discrete_sequence=color_sequence)
                            st.plotly_chart(fig_pie_category)
                            save_chart_to_slide(presentation, fig_pie_category, f'Distribusi Data {item}',category_count)
                    elif idx % 3 == 1:
                        with col1:
                            fig_bar_category = px.bar(category_count, x='Count', y=item, orientation='h', color_discrete_sequence=color_sequence)
                            st.plotly_chart(fig_bar_category)
                            save_chart_to_slide(presentation, fig_bar_category, f'Distribusi Data {item}',category_count)
                        with col2:
                            st.dataframe(category_count, hide_index=True)
                    else:
                        with col2:
                            fig_bar_category = px.bar(category_count, y='Count', x=item, orientation='v', color_discrete_sequence=color_sequence)
                            st.plotly_chart(fig_bar_category)
                            save_chart_to_slide(presentation, fig_bar_category, f'Distribusi Data {item}',category_count)
                        with col1:
                            st.dataframe(category_count, hide_index=True)
        
        if "presentation" not in st.session_state:
            st.session_state["presentation"] = None

        st.session_state["presentation"] = presentation
        # with tempfile.NamedTemporaryFile(suffix=".pptx", delete=False) as ppt_file:
            # presentation.save(ppt_file.name)
            # st.download_button("Download PowerPoint", ppt_file.name, file_name="dashboard_presentation.pptx")

        
        # presentation.save("test.pptx")

    with main_tabs[1]:
        top_section = st.columns([1,2.2], gap="small")
        list_kolom = collection.find_one().keys()

        with top_section[0]:
            # Date filter options
            with st.container(border=True, height=700):
                st.write("##### Filter Data Table :")

                search_query = st.text_input("Search Data Posting :", placeholder="Cari data postingan disini ...")

                filter_option["list_kolom"] = st.multiselect("Kolom di tampilkan", list_kolom, ["Akun/Judul", "Konten", "Sumber"])

                excluded_columns = ["_id", "UUID", "Topik", "Konten"]
                columns = [key for key in list_kolom if key not in excluded_columns]
                sort_by = st.selectbox("Sort By :", options=columns)
                
                sort_dir = st.radio("Direction", options=["🔼", "🔽"], horizontal=True)

                filter_option["tanggal"] = st.date_input('Rentang Waktu :', [datetime.datetime(2024, 7, 1), datetime.datetime.now()])

                # st.write("Jenis Akun :")
                
                with st.expander("Jenis Akun:", expanded=True):
                    selected_jenis_akun = []
                    options = ["Akun Loker", "Akun Netizen", "Akun Pers"]

                    for option in options:
                        if st.checkbox(option, value=True):
                            selected_jenis_akun.append(option)

                filter_option["jenis_akun"] = selected_jenis_akun

                # st.write("Sumber Sosmed :")
                with st.expander("Sumber Sosmed:", expanded=False):
                    selected_sumber = []
                    options = ["Twitter", "Instagram", "Facebook", "Tiktok", "Linkedin", "Youtube"]

                    for option in options:
                        if st.checkbox(option,value=True):
                            selected_sumber.append(option)

                filter_option["sumber"] = selected_sumber

        with top_section[1]:
            with st.container(border=True):
                st.write("##### Scrapping Data Table :")

                with st.spinner('Preparing data.'):
                    try:
                        down_menu = st.columns([1,4,1.5])

                        with down_menu[0]:
                            page_size = st.selectbox("Rows per page", options=[50, 100, 150, 200, 250])
                            total_docs = collection.count_documents({})
                            total_pages = round(total_docs / page_size)

                        with down_menu[1]:
                            st.text("")

                        with down_menu[2]:
                            page = st.number_input("Page", min_value=1, max_value=total_pages, step=1, key="a")

                        # Fetch and display the data
                        df = fetch_data(search_query, sort_by, sort_dir, filter_option, page, page_size)
                        st.dataframe(df, use_container_width=True, height=500)

                        down_menu = st.columns([2,4,1])
                        with down_menu[0]:
                            st.write(f"**Total Data**: {total_docs}")
                        with down_menu[1]:
                            st.text("")
                        with down_menu[2]:
                            st.write(f"**Page**: {page} / {total_pages}")
                    except Exception as e:
                        st.info("")
                        st.error("Gagal Menyiapkan Data." + str(e))