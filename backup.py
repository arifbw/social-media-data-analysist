st.header('Klasifikasi Akun :')

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