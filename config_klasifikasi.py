def konfig_klasifikasi_bw001(idx, config, st_tags):
    if config:
        data_default_akun_loker = config["kat_akun_loker"]
    else:
        data_default_akun_loker = ['loker', 'karir', 'kerja']

    kat_akun_loker = st_tags(
        label="",
        text='Press enter to add more',
        value=data_default_akun_loker,
        maxtags = 200,
        key=f'tags_kat_akun_loker_{idx}')
    
def konfig_klasifikasi_bw004(idx, config, st):
    visible_sheets = []

    if config:
        if config["keywords"] == visible_sheets:
            data_default_keywords = config["keywords"]
        else: 
            data_default_keywords = visible_sheets
    else:
        data_default_keywords = visible_sheets

    keywords = st.multiselect(
        "Kamus Data Analisa Semantik terkait Lowongan :",
        options=visible_sheets,
        default=data_default_keywords,
        key=f"keywords-bw004-{idx}"
    )
def konfig_klasifikasi_bw006(idx, config, st_tags):
    nilai_rentang_gaji = {
        "pre": [],
        "succ": []
    }

    if config:
        data_default_nilai_rentang_gaji = config["nilai_rentang_gaji"]
    else:
        data_default_nilai_rentang_gaji = nilai_rentang_gaji

    nilai_rentang_gaji["pre"] = st_tags(
        label="Kata atau Kalimat Awalan (Preceding) : ",
        text='Press enter to add more',
        maxtags = 200,
        value=data_default_nilai_rentang_gaji["pre"],
        key=f'pre-bw006-{idx}'
    )
    
    nilai_rentang_gaji["succ"] = st_tags(
        label="Kata atau Kalimat Akhiran (Succeding) : ",
        text='Press enter to add more',
        maxtags = 200,
        value=data_default_nilai_rentang_gaji["succ"],
        key=f'succ-bw006-{idx}'
    )

