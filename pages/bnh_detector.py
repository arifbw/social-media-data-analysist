import streamlit as st
import streamlit_authenticator as stauth
from PIL import Image
import page_service as ps


st.set_page_config(
    page_title="Box and Hoax Detector",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="auto"
)

img_logo = Image.open('logo_pasker.png')
st.logo(img_logo, size="large")

if 'auth_obj' in st.session_state:
    authenticator = st.session_state["auth_obj"]

ps.setup_st_sidebar(st, authenticator)

st.write("# Under development")