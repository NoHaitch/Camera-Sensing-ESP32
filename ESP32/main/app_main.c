#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <stdbool.h>
#include <time.h>
#include <sys/time.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "esp_event.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "nvs_flash.h"
#include "sdkconfig.h"

#include "protocol_examples_common.h"

#include "esp_crt_bundle.h"
#include "mqtt_client.h"

#include "esp_sntp.h"

static const char *TAG = "mqtt_image";

extern const uint8_t frame_00_jpg_start[] asm("_binary_frame_00_jpg_start");
extern const uint8_t frame_00_jpg_end[]   asm("_binary_frame_00_jpg_end");
extern const uint8_t frame_01_jpg_start[] asm("_binary_frame_01_jpg_start");
extern const uint8_t frame_01_jpg_end[]   asm("_binary_frame_01_jpg_end");
extern const uint8_t frame_02_jpg_start[] asm("_binary_frame_02_jpg_start");
extern const uint8_t frame_02_jpg_end[]   asm("_binary_frame_02_jpg_end");
extern const uint8_t frame_03_jpg_start[] asm("_binary_frame_03_jpg_start");
extern const uint8_t frame_03_jpg_end[]   asm("_binary_frame_03_jpg_end");
extern const uint8_t frame_04_jpg_start[] asm("_binary_frame_04_jpg_start");
extern const uint8_t frame_04_jpg_end[]   asm("_binary_frame_04_jpg_end");
extern const uint8_t frame_05_jpg_start[] asm("_binary_frame_05_jpg_start");
extern const uint8_t frame_05_jpg_end[]   asm("_binary_frame_05_jpg_end");
extern const uint8_t frame_06_jpg_start[] asm("_binary_frame_06_jpg_start");
extern const uint8_t frame_06_jpg_end[]   asm("_binary_frame_06_jpg_end");
extern const uint8_t frame_07_jpg_start[] asm("_binary_frame_07_jpg_start");
extern const uint8_t frame_07_jpg_end[]   asm("_binary_frame_07_jpg_end");

static esp_mqtt_client_handle_t g_client = NULL;
static bool g_mqtt_connected = false;
static bool g_publish_task_started = false;

static const int T_SECONDS = 2;   // NIM 13522091 -> mod(91,10)+1 = 2
static const int N_SEND = 10;   // 10 / 20 / 100 untuk eksperimen Leve

static const int FRAME_DELAY_MS = 500;
static const int EVENT_COOLDOWN_MS = 5000;

static const int BYTE_DIFF_STEP = 32;
static const int BYTE_DIFF_THRESHOLD = 20;
static const float MOTION_RATIO_THRESHOLD = 0.08f;

static const char *DEVICE_ID = "esp32-13522091";
static const char *MODE = "event";   // "experiment" / "event"

static char g_session_id[32];

typedef struct {
    const uint8_t *start;
    const uint8_t *end;
    const char *name;
} embedded_frame_t;

static const embedded_frame_t g_event_frames[] = {
    { frame_00_jpg_start, frame_00_jpg_end, "frame_00.jpg" },
    { frame_01_jpg_start, frame_01_jpg_end, "frame_01.jpg" },
    { frame_02_jpg_start, frame_02_jpg_end, "frame_02.jpg" },
    { frame_03_jpg_start, frame_03_jpg_end, "frame_03.jpg" },
    { frame_04_jpg_start, frame_04_jpg_end, "frame_04.jpg" },
    { frame_05_jpg_start, frame_05_jpg_end, "frame_05.jpg" },
    { frame_06_jpg_start, frame_06_jpg_end, "frame_06.jpg" },
    { frame_07_jpg_start, frame_07_jpg_end, "frame_07.jpg" },
};

static const size_t g_event_frame_count = sizeof(g_event_frames) / sizeof(g_event_frames[0]);

static int64_t get_epoch_time_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return ((int64_t)tv.tv_sec * 1000000LL) + (int64_t)tv.tv_usec;
}

static bool is_time_synchronized(void) {
    time_t now;
    time(&now);
    struct tm timeinfo = {0};
    localtime_r(&now, &timeinfo);
    return (timeinfo.tm_year >= (2024 - 1900));
}

static void sync_time_with_sntp(void) {
    ESP_LOGI(TAG, "Initializing SNTP");

    esp_sntp_setoperatingmode(SNTP_OPMODE_POLL);
    esp_sntp_setservername(0, "pool.ntp.org");
    esp_sntp_setservername(1, "time.google.com");
    esp_sntp_init();

    const int max_retry = 30;
    int retry = 0;

    while (!is_time_synchronized() && retry < max_retry) {
        ESP_LOGI(TAG, "Waiting for SNTP time sync... (%d/%d)", retry + 1, max_retry);
        vTaskDelay(pdMS_TO_TICKS(1000));
        retry++;
    }

    if (!is_time_synchronized()) {
        ESP_LOGE(TAG, "Failed to synchronize time with SNTP");
        return;
    }

    time_t now;
    time(&now);
    struct tm timeinfo = {0};
    localtime_r(&now, &timeinfo);

    char time_str[64];
    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &timeinfo);

    ESP_LOGI(TAG, "SNTP time synchronized: %s", time_str);
    ESP_LOGI(TAG, "Epoch time (us): %" PRId64, get_epoch_time_us());
}

static void build_session_id(char *buf, size_t len) {
    int64_t epoch_us = get_epoch_time_us();
    snprintf(buf, len, "sess-%" PRId64, epoch_us);
}

static void build_experiment_id(char *buf, size_t len) {
    snprintf(buf, len, "exp-n%d-t%d", N_SEND, T_SECONDS);
}

static void build_topic(char *buf, size_t len, const char *suffix) {
    snprintf(buf, len, "%s/%s", CONFIG_APP_MQTT_TOPIC, suffix);
}

static size_t get_image_size_bytes(const uint8_t *start, const uint8_t *end) {
    return (size_t)(end - start);
}

static void log_experiment_header(size_t image_size_bytes) {
    char experiment_id[64];
    build_experiment_id(experiment_id, sizeof(experiment_id));

    ESP_LOGI(TAG, "==================================================");
    ESP_LOGI(TAG, "LEVEL 1 IMAGE MQTT EXPERIMENT");
    ESP_LOGI(TAG, "device_id     : %s", DEVICE_ID);
    ESP_LOGI(TAG, "session_id    : %s", g_session_id);
    ESP_LOGI(TAG, "mode          : %s", MODE);
    ESP_LOGI(TAG, "experiment_id : %s", experiment_id);
    ESP_LOGI(TAG, "interval_s    : %d", T_SECONDS);
    ESP_LOGI(TAG, "attempt_total : %d", N_SEND);
    ESP_LOGI(TAG, "image_size    : %u bytes", (unsigned int)image_size_bytes);
    ESP_LOGI(TAG, "base_topic    : %s", CONFIG_APP_MQTT_TOPIC);
    ESP_LOGI(TAG, "==================================================");
}

static void log_event_header(void) {
    ESP_LOGI(TAG, "==================================================");
    ESP_LOGI(TAG, "LEVEL 2 EVENT-BASED SIMULATION");
    ESP_LOGI(TAG, "device_id              : %s", DEVICE_ID);
    ESP_LOGI(TAG, "session_id             : %s", g_session_id);
    ESP_LOGI(TAG, "mode                   : %s", MODE);
    ESP_LOGI(TAG, "frame_count            : %u", (unsigned int)g_event_frame_count);
    ESP_LOGI(TAG, "frame_delay_ms         : %d", FRAME_DELAY_MS);
    ESP_LOGI(TAG, "event_cooldown_ms      : %d", EVENT_COOLDOWN_MS);
    ESP_LOGI(TAG, "byte_diff_step         : %d", BYTE_DIFF_STEP);
    ESP_LOGI(TAG, "byte_diff_threshold    : %d", BYTE_DIFF_THRESHOLD);
    ESP_LOGI(TAG, "motion_ratio_threshold : %.3f", (double)MOTION_RATIO_THRESHOLD);
    ESP_LOGI(TAG, "base_topic             : %s", CONFIG_APP_MQTT_TOPIC);
    ESP_LOGI(TAG, "==================================================");
}

static void publish_image_buffer(
    esp_mqtt_client_handle_t client,
    const uint8_t *image_data,
    size_t image_size_bytes,
    int attempt_index,
    int attempt_total,
    const char *experiment_id
) {
    char meta_topic[128];
    char raw_topic[128];
    build_topic(meta_topic, sizeof(meta_topic), "meta");
    build_topic(raw_topic, sizeof(raw_topic), "raw");

    int64_t ts_send_us = get_epoch_time_us();

    char meta_payload[512];
    snprintf(meta_payload, sizeof(meta_payload),
             "{"
             "\"device_id\":\"%s\","
             "\"session_id\":\"%s\","
             "\"mode\":\"%s\","
             "\"experiment_id\":\"%s\","
             "\"attempt_index\":%d,"
             "\"attempt_total\":%d,"
             "\"timestamp_us\":%" PRId64 ","
             "\"size_bytes\":%u"
             "}",
             DEVICE_ID,
             g_session_id,
             MODE,
             experiment_id,
             attempt_index,
             attempt_total,
             ts_send_us,
             (unsigned int)image_size_bytes);

    ESP_LOGI(TAG, "--------------------------------------------------");
    ESP_LOGI(TAG, "SEND %d/%d", attempt_index, attempt_total);
    ESP_LOGI(TAG, "meta_topic    : %s", meta_topic);
    ESP_LOGI(TAG, "raw_topic     : %s", raw_topic);
    ESP_LOGI(TAG, "timestamp_us  : %" PRId64, ts_send_us);
    ESP_LOGI(TAG, "metadata_json : %s", meta_payload);
    ESP_LOGI(TAG, "raw_size      : %u bytes", (unsigned int)image_size_bytes);

    int meta_msg_id = esp_mqtt_client_publish(client, meta_topic, meta_payload, 0, 1, 0);
    int raw_msg_id  = esp_mqtt_client_publish(client, raw_topic, (const char *)image_data, (int)image_size_bytes, 1, 0);

    ESP_LOGI(TAG, "publish_meta_msg_id : %d", meta_msg_id);
    ESP_LOGI(TAG, "publish_raw_msg_id  : %d", raw_msg_id);
    ESP_LOGI(TAG, "--------------------------------------------------");
}

static void publish_level1_image_once(esp_mqtt_client_handle_t client, int attempt_index) {
    const uint8_t *image_data = frame_00_jpg_start;
    size_t image_size_bytes = get_image_size_bytes(frame_00_jpg_start, frame_00_jpg_end);

    char experiment_id[64];
    build_experiment_id(experiment_id, sizeof(experiment_id));

    publish_image_buffer(client, image_data, image_size_bytes, attempt_index, N_SEND, experiment_id);
}

static float compute_frame_motion_score(
    const uint8_t *prev_data, size_t prev_size,
    const uint8_t *curr_data, size_t curr_size
) {
    size_t min_size = (prev_size < curr_size) ? prev_size : curr_size;
    if (min_size == 0) {
        return 0.0f;
    }

    size_t changed = 0;
    size_t sampled = 0;

    for (size_t i = 0; i < min_size; i += BYTE_DIFF_STEP) {
        int diff = (int)curr_data[i] - (int)prev_data[i];
        if (diff < 0) diff = -diff;

        if (diff >= BYTE_DIFF_THRESHOLD) {
            changed++;
        }
        sampled++;
    }

    if (sampled == 0) {
        return 0.0f;
    }

    return (float)changed / (float)sampled;
}

static bool is_motion_detected(
    const uint8_t *prev_data, size_t prev_size,
    const uint8_t *curr_data, size_t curr_size,
    float *out_score
) {
    float score = compute_frame_motion_score(prev_data, prev_size, curr_data, curr_size);
    if (out_score) {
        *out_score = score;
    }
    return (score >= MOTION_RATIO_THRESHOLD);
}

static void run_experiment_mode(void) {
    const size_t image_size_bytes = get_image_size_bytes(frame_00_jpg_start, frame_00_jpg_end);
    log_experiment_header(image_size_bytes);

    for (int attempt_index = 1; attempt_index <= N_SEND; attempt_index++) {
        if (!g_mqtt_connected) {
            ESP_LOGW(TAG, "MQTT disconnected, stop sending");
            break;
        }

        publish_level1_image_once(g_client, attempt_index);

        if (attempt_index < N_SEND) {
            vTaskDelay(pdMS_TO_TICKS(T_SECONDS * 1000));
        }
    }

    ESP_LOGI(TAG, "Experiment finished");
}

static void run_event_mode(void) {
    int frame_index = 0;
    int send_index = 1;
    int64_t last_event_us = 0;
    bool has_prev_frame = false;
    const uint8_t *prev_data = NULL;
    size_t prev_size = 0;

    char experiment_id[64];
    snprintf(experiment_id, sizeof(experiment_id), "event-motion-cooldown-%dms", EVENT_COOLDOWN_MS);

    log_event_header();

    while (g_mqtt_connected) {
        const embedded_frame_t *frame = &g_event_frames[frame_index];
        const uint8_t *curr_data = frame->start;
        size_t curr_size = get_image_size_bytes(frame->start, frame->end);

        float motion_score = 0.0f;
        bool motion = false;

        if (has_prev_frame) {
            motion = is_motion_detected(prev_data, prev_size, curr_data, curr_size, &motion_score);
        }

        int64_t now_us = get_epoch_time_us();
        bool cooldown_ok = (last_event_us == 0) ||
                           ((now_us - last_event_us) >= ((int64_t)EVENT_COOLDOWN_MS * 1000LL));

        ESP_LOGI(TAG,
                 "FRAME %d/%u | %s | size=%u | motion=%s | score=%.4f | cooldown_ok=%s",
                 frame_index + 1,
                 (unsigned int)g_event_frame_count,
                 frame->name,
                 (unsigned int)curr_size,
                 motion ? "YES" : "NO",
                 (double)motion_score,
                 cooldown_ok ? "YES" : "NO");

        if (has_prev_frame && motion && cooldown_ok) {
            ESP_LOGI(TAG, "EVENT TRIGGERED -> publish frame %s", frame->name);
            publish_image_buffer(g_client, curr_data, curr_size, send_index++, 999999, experiment_id);
            last_event_us = now_us;
        } else if (has_prev_frame && motion && !cooldown_ok) {
            int64_t remain_ms = EVENT_COOLDOWN_MS - ((now_us - last_event_us) / 1000LL);
            if (remain_ms < 0) remain_ms = 0;
            ESP_LOGI(TAG, "MOTION DETECTED but cooldown active, remaining=%" PRId64 " ms", remain_ms);
        }

        prev_data = curr_data;
        prev_size = curr_size;
        has_prev_frame = true;

        frame_index = (frame_index + 1) % g_event_frame_count;
        vTaskDelay(pdMS_TO_TICKS(FRAME_DELAY_MS));
    }

    ESP_LOGI(TAG, "Event mode stopped");
}

static void image_publish_task(void *pvParameters) {
    if (strcmp(MODE, "experiment") == 0) {
        run_experiment_mode();
    } else if (strcmp(MODE, "event") == 0) {
        run_event_mode();
    } else {
        ESP_LOGE(TAG, "Unknown MODE: %s", MODE);
    }

    g_publish_task_started = false;
    vTaskDelete(NULL);
}

static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data) {
    esp_mqtt_event_handle_t event = (esp_mqtt_event_handle_t)event_data;
    esp_mqtt_client_handle_t client = event->client;

    switch ((esp_mqtt_event_id_t)event_id) {
    case MQTT_EVENT_CONNECTED:
        ESP_LOGI(TAG, "MQTT connected");
        g_client = client;
        g_mqtt_connected = true;

        if (!g_publish_task_started) {
            g_publish_task_started = true;
            xTaskCreate(image_publish_task, "image_publish_task", 8192, NULL, 5, NULL);
        }
        break;

    case MQTT_EVENT_DISCONNECTED:
        ESP_LOGW(TAG, "MQTT disconnected");
        g_mqtt_connected = false;
        break;

    case MQTT_EVENT_PUBLISHED:
        ESP_LOGI(TAG, "MQTT_EVENT_PUBLISHED msg_id=%d", event->msg_id);
        break;

    case MQTT_EVENT_ERROR:
        ESP_LOGE(TAG, "MQTT error");
        g_mqtt_connected = false;
        break;

    default:
        break;
    }
}

static void mqtt_app_start(void) {
    const esp_mqtt_client_config_t mqtt_cfg = {
        .broker = {
            .address.uri = CONFIG_APP_MQTT_BROKER_URI,
            .verification.crt_bundle_attach = esp_crt_bundle_attach,
        },
        .credentials = {
            .username = CONFIG_APP_MQTT_USERNAME,
            .authentication.password = CONFIG_APP_MQTT_PASSWORD,
        },
    };

    esp_mqtt_client_handle_t client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(client);
}

void app_main(void) {
    ESP_LOGI(TAG, "Starting app");
    ESP_LOGI(TAG, "ESP-IDF version : %s", esp_get_idf_version());
    ESP_LOGI(TAG, "T_SECONDS       : %d", T_SECONDS);
    ESP_LOGI(TAG, "N_SEND          : %d", N_SEND);
    ESP_LOGI(TAG, "FRAME_DELAY_MS  : %d", FRAME_DELAY_MS);
    ESP_LOGI(TAG, "EVENT_COOLDOWN_MS : %d", EVENT_COOLDOWN_MS);
    ESP_LOGI(TAG, "MODE            : %s", MODE);

    ESP_ERROR_CHECK(nvs_flash_init());
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    ESP_ERROR_CHECK(example_connect());

    sync_time_with_sntp();

    if (!is_time_synchronized()) {
        ESP_LOGE(TAG, "System time is invalid, abort publishing");
        return;
    }

    build_session_id(g_session_id, sizeof(g_session_id));
    ESP_LOGI(TAG, "session_id      : %s", g_session_id);

    mqtt_app_start();
}
