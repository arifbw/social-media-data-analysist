import streamlit as st
from PIL import Image
from streamlit_jupyter import StreamlitPatcher, tqdm

st.set_page_config(
    page_title="Hello",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="auto"
)

img_logo = Image.open('logo_pasker.png')
st.logo(img_logo, size="large")

st.markdown("""
        <style>
            div[data-testid="stSidebarNav"] {
                display: none;
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
            .stAppHeader{
                background: #ffe0dc;
                box-shadow: 0px 5px 10px 1px rgba(0, 0, 0, 0.2);
            }
        </style>
    """, unsafe_allow_html=True)

if st.sidebar.button("Back to Home", type="primary", icon=":material/arrow_back:", use_container_width=True):
    st.switch_page("streamlit_app.py")

st.write("# Welcome to Page Jupyter! 👋")



StreamlitPatcher().jupyter() 