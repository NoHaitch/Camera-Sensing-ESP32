idf_component_register(
    SRCS "app_main.c"
    PRIV_REQUIRES esp-tls esp_wifi esp_event nvs_flash
    INCLUDE_DIRS "."
    EMBED_FILES
        frame_00.jpg
        frame_01.jpg
        frame_02.jpg
        frame_03.jpg
        frame_04.jpg
        frame_05.jpg
        frame_06.jpg
        frame_07.jpg
)